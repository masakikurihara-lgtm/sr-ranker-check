import streamlit as st
import requests
import pandas as pd
import io
import datetime
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from ftplib import FTP
from io import StringIO, BytesIO

JST = datetime.timezone(datetime.timedelta(hours=9))

# --- 設定 ---
st.set_page_config(page_title="SHOWROOM 高精度・ID蓄積型巡回ツール", layout="wide")

ROOM_LIST_URL = "https://mksoul-pro.com/showroom/file/room_list.csv"
EVENT_SEARCH_API = "https://www.showroom-live.com/api/event/search"
EVENT_ROOM_LIST_API = "https://www.showroom-live.com/api/event/room_list"
ROOM_PROFILE_API = "https://www.showroom-live.com/api/room/profile?room_id={room_id}"
FTP_FILE_PATH = "/mksoul-pro.com/showroom/file/ranker_liver_list.csv"

GENRE_MAP = {
    112: "ミュージック", 102: "アイドル", 103: "タレント", 104: "声優",
    105: "芸人", 107: "バーチャル", 108: "モデル", 109: "俳優",
    110: "アナウンサー", 113: "クリエイター", 200: "ライバー",
}

RANK_ORDER = ["SS-5", "SS-4", "SS-3", "SS-2", "SS-1", "S-5", "S-4", "S-3", "S-2", "S-1", "A-5", "A-4", "A-3", "A-2", "A-1", "B-5"]

# --- 通信セッション ---
def create_session():
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124 Safari/537.36'})
    return session

# --- FTP関連関数 ---
def get_ftp_connection():
    ftp_host = st.secrets["ftp"]["host"]
    ftp_user = st.secrets["ftp"]["user"]
    ftp_pass = st.secrets["ftp"]["password"]
    ftp = FTP(ftp_host)
    ftp.login(user=ftp_user, passwd=ftp_pass)
    ftp.set_pasv(True)
    return ftp

def download_ranker_ids(ftp):
    """FTPから蓄積された名簿IDを取得"""
    try:
        r = StringIO()
        ftp.retrlines(f'RETR {FTP_FILE_PATH}', lambda x: r.write(x + '\n'))
        r.seek(0)
        df = pd.read_csv(r, header=None, dtype=str)
        return set(df[0].dropna().unique().tolist())
    except Exception:
        return set()

def upload_ranker_ids(ftp, id_set):
    """マージ済みのIDセットをFTPに保存"""
    try:
        if not id_set: return
        # 保存前にIDをソート（管理しやすくするため）
        sorted_ids = sorted(list(id_set), key=lambda x: int(x) if x.isdigit() else 0)
        df = pd.DataFrame(sorted_ids)
        csv_string = df.to_csv(index=False, header=False, encoding='utf-8')
        byte_buffer = BytesIO(csv_string.encode('utf-8'))
        ftp.storbinary(f'STOR {FTP_FILE_PATH}', byte_buffer)
        st.success(f"✅ 名簿を蓄積・更新しました（累計 {len(id_set)} 件）")
    except Exception as e:
        st.error(f"FTP保存エラー: {e}")

# --- API抽出ロジック ---
def get_event_ids(session):
    event_ids = set()
    for status in [1, 3, 4]:
        page = 1
        while page <= 5: 
            try:
                res = session.get(f"{EVENT_SEARCH_API}?status={status}&page={page}", timeout=10)
                data = res.json()
                items = data.get("event_list", [])
                if not items: break
                for item in items:
                    eid = item.get("event_id")
                    if eid: event_ids.add(str(eid))
                page += 1
            except: break
    return list(event_ids)

def get_room_ids_from_event(session, event_id):
    room_ids = set()
    page = 1
    while True:
        try:
            res = session.get(f"{EVENT_ROOM_LIST_API}?event_id={event_id}&p={page}", timeout=10)
            data = res.json()
            room_list = data.get("list", [])
            if not room_list: break
            for r in room_list:
                rid = r.get("room_id")
                if rid: room_ids.add(str(rid))
            if not data.get("next_page") or data.get("next_page") <= page: break
            page = data.get("next_page")
            time.sleep(0.05)
        except: break
    return room_ids

def get_room_profile(room_id, session):
    url = ROOM_PROFILE_API.format(room_id=room_id)
    try:
        response = session.get(url, timeout=10)
        data = response.json()
        return room_id, data, "成功" if data else "空データ"
    except Exception as e:
        return room_id, None, str(e)

def _safe_get(data, keys, default_value=None):
    temp = data
    for key in keys:
        if isinstance(temp, dict) and key in temp: temp = temp.get(key)
        else: return default_value
    return temp if temp not in [None, "", " "] else default_value

# --- メイン処理 ---
def process_status_check(id_list, update_ftp=False, existing_past_ids=None):
    if not id_list:
        st.warning("処理対象のルームIDがありません。")
        return

    all_results = {}
    error_log = {}
    session = create_session()
    
    st.info(f"合計 {len(id_list)} 件を精査します...")
    progress_bar = st.progress(0)
    status_text = st.empty()

    with ThreadPoolExecutor(max_workers=40) as executor:
        futures = {executor.submit(get_room_profile, rid, session): rid for rid in id_list}
        for i, future in enumerate(as_completed(futures)):
            rid, res, msg = future.result()
            if res: all_results[rid] = res
            else: error_log[rid] = msg
            if i % 50 == 0 or i == len(id_list)-1:
                progress_bar.progress((i + 1) / len(id_list))
                status_text.text(f"進捗: {i+1} / {len(id_list)}")
    
    progress_bar.empty()
    status_text.empty()
    
    display_results(all_results, error_log, update_ftp, existing_past_ids)

def display_results(all_room_data, error_log, update_ftp, existing_past_ids):
    st.markdown("""<style>
        .result-table { width: 100%; border-collapse: collapse; font-size: 14px; }
        .result-table th { background-color: #f0f2f6; position: sticky; top: 0; padding: 10px; border: 1px solid #ddd; }
        .result-table td { padding: 8px; border: 1px solid #ddd; text-align: center; }
        .hl-up { background-color: #e3f2fd; }
        .hl-low { background-color: #fff9c4; }
    </style>""", unsafe_allow_html=True)

    processed = []
    found_b5_above_ids = set()
    low_rank_count = 0

    for rid, p in all_room_data.items():
        rank = _safe_get(p, ["show_rank_subdivided"], "-")
        if rank in RANK_ORDER:
            found_b5_above_ids.add(str(rid))
            processed.append({
                "rid": rid, "p": p, "rank_idx": RANK_ORDER.index(rank),
                "next": int(_safe_get(p, ["next_score"], 99999999))
            })
        else:
            low_rank_count += 1

    # 蓄積ロジック: 既存名簿に、今回新たに見つかったB-5以上を合体させて保存
    if update_ftp:
        base_ids = existing_past_ids if existing_past_ids else set()
        merged_ids = base_ids.union(found_b5_above_ids)
        try:
            with get_ftp_connection() as ftp:
                upload_ranker_ids(ftp, merged_ids)
        except Exception as e:
            st.error(f"FTP蓄積エラー: {e}")

    processed.sort(key=lambda x: (x["rank_idx"], x["next"]))
    st.success(f"【判定完了】 B-5以上: {len(processed)}件 / ランク外: {low_rank_count}件 / 失敗: {len(error_log)}件")
    
    if processed:
        headers = ["ルーム名", "レベル", "SHOWランク", "上位まで", "下位まで", "フォロワー", "継続日数", "ジャンル"]
        rows = []
        csv_rows = []
        for item in processed:
            p = item["p"]
            rid = item["rid"]
            name, rank = _safe_get(p, ["room_name"], "Unknown"), _safe_get(p, ["show_rank_subdivided"], "-")
            n_score, p_score = _safe_get(p, ["next_score"], "-"), _safe_get(p, ["prev_score"], "-")
            url = f"https://www.showroom-live.com/room/profile?room_id={rid}"
            rows.append(f"<tr><td><a href='{url}' target='_blank'>{name}</a></td><td>{p.get('room_level','-')}</td><td>{rank}</td><td class='{'hl-up' if str(n_score).isdigit() and int(n_score)<=30000 else ''}'>{n_score}</td><td class='{'hl-low' if str(p_score).isdigit() and int(p_score)<=30000 else ''}'>{p_score}</td><td>{p.get('follower_num','-')}</td><td>{p.get('live_continuous_days','-')}</td><td>{GENRE_MAP.get(p.get('genre_id'),'-')}</td></tr>")
            csv_rows.append([name, p.get('room_level'), rank, n_score, p_score, p.get('follower_num'), p.get('live_continuous_days'), GENRE_MAP.get(p.get('genre_id'))])
        st.markdown(f'<table class="result-table"><thead>{"".join(f"<th>{h}</th>" for h in headers)}</thead><tbody>{"".join(rows)}</tbody></table>', unsafe_allow_html=True)
        st.download_button("📥 結果CSVを保存", pd.DataFrame(csv_rows, columns=headers).to_csv(index=False).encode('utf-8-sig'), "showroom_results.csv", "text/csv")

# --- UI ---
if 'authenticated' not in st.session_state: st.session_state.authenticated = False
if not st.session_state.authenticated:
    st.title("💖 SHOWROOM 統合管理ツール")
    auth_code = st.text_input("認証コード:", type="password")
    if st.button("ログイン"):
        try:
            res = requests.get(ROOM_LIST_URL)
            if auth_code in res.text:
                st.session_state.authenticated = True
                st.rerun()
        except: st.error("認証エラー")
    st.stop()

st.title("💖 SHOWROOM ステータス自動巡回ツール")
tab1, tab2 = st.tabs(["自動スキャン（イベント＋名簿蓄積）", "手動ID入力"])

with tab1:
    st.markdown("既存名簿を維持しつつ、新しいイベント参加者からB-5以上を「追加」して蓄積します。")
    if st.button("🚀 スキャン開始（名簿蓄積実行）"):
        session = create_session()
        with st.spinner("名簿を読み込み中..."):
            try:
                with get_ftp_connection() as ftp:
                    past_ids = download_ranker_ids(ftp)
                st.write(f"📁 現在の名簿数: {len(past_ids)} 件")
            except:
                past_ids = set()
                st.info("新規名簿として開始します。")

        with st.spinner("最新イベントを検索中..."):
            event_ids = get_event_ids(session)
            event_room_ids = set()
            p_evt = st.progress(0)
            for i, eid in enumerate(event_ids):
                event_room_ids.update(get_room_ids_from_event(session, eid))
                p_evt.progress((i + 1) / len(event_ids))
        
        # 既存名簿 ＋ 今回のイベント参加者（重複なし）
        total_unique_ids = list(past_ids.union(event_room_ids))
        st.write(f"✨ 今回の新規イベント参加者: {len(event_room_ids)} 件")
        st.write(f"🔄 検索対象（名簿＋イベント）: {len(total_unique_ids)} 件")
        
        # process_status_checkに既存の名簿を渡して、合体保存できるようにする
        process_status_check(total_unique_ids, update_ftp=True, existing_past_ids=past_ids)

with tab2:
    room_ids_raw = st.text_area("ルームIDを入力:", height=200)
    if st.button("🔍 指定IDのみチェック"):
        id_list = list(set([rid.strip() for rid in re.split(r'[,\s\n]+', room_ids_raw) if rid.strip().isdigit()]))
        process_status_check(id_list, update_ftp=False)