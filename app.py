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
st.set_page_config(
    page_title="SHOWROOM ランカーチェッカー",
    layout="wide"
)

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
    try:
        r = StringIO()
        ftp.retrlines(f'RETR {FTP_FILE_PATH}', lambda x: r.write(x + '\n'))
        r.seek(0)
        df = pd.read_csv(r, header=None, dtype=str)
        return set(df[0].dropna().unique().tolist())
    except Exception:
        return set()

def upload_ranker_ids(ftp, id_set):
    try:
        if not id_set: return
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
        return response.json()
    except:
        return None

def _safe_get(data, keys, default_value=None):
    temp = data
    for key in keys:
        if isinstance(temp, dict) and key in temp: temp = temp.get(key)
        else: return default_value
    if temp is None or (isinstance(temp, str) and temp.strip() == "") or (isinstance(temp, float) and pd.isna(temp)):
        return default_value
    return temp

# --- 表示ロジック ---
def display_multiple_results(all_room_data, update_ftp=False, existing_past_ids=None):
    now_str = datetime.datetime.now(JST).strftime('%Y/%m/%d %H:%M:%S')
    st.caption(f"（取得時刻: {now_str} 現在）")
    
    custom_styles = """
    <style>
    .basic-info-table-wrapper { 
        width: 100%; margin: 0 auto; overflow-y: auto; 
        max-height: 70vh; border: 1px solid #c5cae9;
    }
    .basic-info-table { border-collapse: separate; border-spacing: 0; width: 100%; }
    .basic-info-table th { 
        position: sticky; top: 0; z-index: 10;
        text-align: center !important; background-color: #e8eaf6; 
        color: #1a237e; font-weight: bold; padding: 8px 10px; 
        border-bottom: 1px solid #c5cae9; border-right: 1px solid #c5cae9;
        white-space: nowrap; 
    }
    .basic-info-table td { 
        text-align: center !important; padding: 8px 10px; line-height: 1.4; 
        border-bottom: 1px solid #f0f0f0; border-right: 1px solid #f0f0f0;
        white-space: nowrap; font-weight: 600; 
    }
    .basic-info-table th:last-child, .basic-info-table td:last-child { border-right: none; }
    .basic-info-table tbody tr:hover { background-color: #f7f9fd; }
    .basic-info-highlight-upper { background-color: #e3f2fd !important; color: #0d47a1; }
    .basic-info-highlight-lower { background-color: #fff9c4 !important; color: #795548; }
    /* ランクの境界線スタイル */
    .rank-boundary td { border-bottom: 3px solid #1a237e !important; }
    .room-link { text-decoration: underline; color: #1f2937; }
    </style>
    """
    st.markdown(custom_styles, unsafe_allow_html=True)

    # 順位を追加
    headers = ["順位", "ルーム名", "ユーザーID", "ルームレベル", "現在のSHOWランク", "上位ランクまでのスコア", "下位ランクまでのスコア", "フォロワー数", "まいにち配信", "ジャンル", "公式 or フリー"]

    def is_within_30000(value):
        try: return int(value) <= 30000
        except: return False

    def format_value(value):
        if value == "-" or value is None: return "-"
        try: return f"{int(value):,}"
        except: return str(value)

    processed_list = []
    found_b5_above_ids = set()
    
    for rid, p in all_room_data.items():
        if not p: continue
        rank = _safe_get(p, ["show_rank_subdivided"], "-")
        if rank in RANK_ORDER:
            found_b5_above_ids.add(str(rid))
            processed_list.append({
                "rid": rid, "p": p, "rank_idx": RANK_ORDER.index(rank),
                "next": int(_safe_get(p, ["next_score"], 99999999))
            })

    if update_ftp:
        base_ids = existing_past_ids if existing_past_ids else set()
        merged_ids = base_ids.union(found_b5_above_ids)
        try:
            with get_ftp_connection() as ftp:
                upload_ranker_ids(ftp, merged_ids)
        except Exception as e:
            st.error(f"FTP保存失敗: {e}")

    # ソート実行
    processed_list.sort(key=lambda x: (x["rank_idx"], x["next"]))

    rows_html = []
    csv_data = []

    for idx, item in enumerate(processed_list):
        p = item["p"]
        rid = item["rid"]
        
        name = _safe_get(p, ["room_name"], "取得失敗")
        level = _safe_get(p, ["room_level"], "-")
        rank = _safe_get(p, ["show_rank_subdivided"], "-")
        n_score = _safe_get(p, ["next_score"], "-")
        p_score = _safe_get(p, ["prev_score"], "-")
        fol = _safe_get(p, ["follower_num"], "-")
        days = _safe_get(p, ["live_continuous_days"], "-")
        is_official = _safe_get(p, ["is_official"], None)
        genre_id = _safe_get(p, ["genre_id"], None)

        off_stat = "公式" if is_official is True else "フリー" if is_official is False else "-"
        gen_name = GENRE_MAP.get(genre_id, f"その他 ({genre_id})" if genre_id else "-")
        url = f"https://www.showroom-live.com/room/profile?room_id={rid}"
        
        name_cell = f'<a href="{url}" target="_blank" class="room-link">{name}</a>'
        
        # 表示用リスト（順位を追加）
        rank_num = idx + 1
        display_vals = [rank_num, name_cell, rid, format_value(level), rank, format_value(n_score), format_value(p_score), format_value(fol), format_value(days), gen_name, off_stat]
        
        # 次のアイテムとランクが違うかチェックして境界線クラスを付与
        row_class = ""
        if idx < len(processed_list) - 1:
            if item["rank_idx"] != processed_list[idx+1]["rank_idx"]:
                row_class = ' class="rank-boundary"'

        td_html = []
        for i, val in enumerate(display_vals):
            cls = ""
            if headers[i] == "上位ランクまでのスコア" and is_within_30000(n_score): cls = "basic-info-highlight-upper"
            elif headers[i] == "下位ランクまでのスコア" and is_within_30000(p_score): cls = "basic-info-highlight-lower"
            td_html.append(f'<td class="{cls}">{val}</td>')
        
        rows_html.append(f"<tr{row_class}>{''.join(td_html)}</tr>")
        csv_data.append([rank_num, name, rid, level, rank, n_score, p_score, fol, days, gen_name, off_stat])

    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown("<h1 style='font-size:22px; text-align:left; color:#1f2937; padding: 15px 0px 5px 0px;'>📊 ルーム基本情報一覧</h1>", unsafe_allow_html=True)
    with col2:
        if csv_data:
            df_dl = pd.DataFrame(csv_data, columns=headers)
            st.download_button("📥 CSVをダウンロード", df_dl.to_csv(index=False).encode('utf-8-sig'), f"showroom_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", "text/csv")

    st.markdown(f'<div class="basic-info-table-wrapper"><table class="basic-info-table"><thead><tr>{"".join(f"<th>{h}</th>" for h in headers)}</tr></thead><tbody>{"".join(rows_html)}</tbody></table></div>', unsafe_allow_html=True)

# --- スキャン実行 ---
def run_scan(id_list, update_ftp=False, existing_past_ids=None):
    if not id_list:
        st.warning("処理対象のIDがありません。")
        return
    all_results = {}
    session = create_session()
    st.info(f"合計 {len(id_list)} 件のステータスを確認中...")
    progress_bar = st.progress(0)
    
    with ThreadPoolExecutor(max_workers=40) as executor:
        futures = {executor.submit(get_room_profile, rid, session): rid for rid in id_list}
        for i, future in enumerate(as_completed(futures)):
            rid = futures[future]
            res = future.result()
            if res: all_results[rid] = res
            progress_bar.progress((i + 1) / len(id_list))
    
    display_multiple_results(all_results, update_ftp, existing_past_ids)

# --- 認証 & UI メインロジック ---
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False

if not st.session_state.authenticated:
    st.markdown("<h1 style='font-size:28px; text-align:left; color:#1f2937;'>💖 SHOWROOM ランカーチェッカー</h1>", unsafe_allow_html=True)
    st.markdown("##### 🔑 認証コードを入力してください")
    auth_input = st.text_input("認証コード:", type="password", key="auth_input_field")
    
    if st.button("認証する"):
        if auth_input:
            with st.spinner("認証リストを確認中..."):
                try:
                    response = requests.get(ROOM_LIST_URL, timeout=10)
                    response.raise_for_status()
                    valid_codes = set(str(x).strip() for x in pd.read_csv(io.StringIO(response.text), header=None, dtype=str).iloc[:, 0].dropna())
                    if auth_input.strip() in valid_codes:
                        st.session_state.authenticated = True
                        st.rerun()
                    else:
                        st.error("❌ 認証コードが無効です。")
                except Exception as e:
                    st.error(f"認証リストの取得に失敗しました: {e}")
    st.stop()

st.markdown("<h1 style='font-size:28px; text-align:left; color:#1f2937;'>💖 SHOWROOM ランカーチェッカー</h1>", unsafe_allow_html=True)
tab1, tab2 = st.tabs(["自動スキャン", "手動ID入力"])

with tab1:
    if st.button("🚀 スキャン開始（名簿蓄積実行）"):
        session = create_session()
        with get_ftp_connection() as ftp:
            past_ids = download_ranker_ids(ftp)
        
        st.write(f"📁 現在の名簿数: {len(past_ids)} 件")
        
        with st.spinner("対象ルーム候補を取得中..."):
            event_ids = get_event_ids(session)
        
        event_room_ids = set()
        if event_ids:
            st.info(f"対象ルーム候補を取得しています...")
            ev_progress = st.progress(0)
            for i, eid in enumerate(event_ids):
                event_room_ids.update(get_room_ids_from_event(session, eid))
                ev_progress.progress((i + 1) / len(event_ids))
        
        total_unique_ids = list(past_ids.union(event_room_ids))
        st.write(f"🔄 検索対象合計（重複排除後）: {len(total_unique_ids)} 件")
        
        run_scan(total_unique_ids, update_ftp=True, existing_past_ids=past_ids)

with tab2:
    room_ids_raw = st.text_area("ルームIDを入力:", placeholder="12345, 67890", height=200)
    if st.button("🔍 指定IDのみチェック"):
        id_list = list(set([rid.strip() for rid in re.split(r'[,\s\n]+', room_ids_raw) if rid.strip().isdigit()]))
        run_scan(id_list, update_ftp=False)