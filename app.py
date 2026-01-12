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

JST = datetime.timezone(datetime.timedelta(hours=9))

# --- 設定 ---
st.set_page_config(page_title="SHOWROOM 統合ステータス確認ツール", layout="wide")

ROOM_LIST_URL = "https://mksoul-pro.com/showroom/file/room_list.csv"
EVENT_SEARCH_API = "https://www.showroom-live.com/api/event/search"
EVENT_ROOM_LIST_API = "https://www.showroom-live.com/api/event/room_list"
ROOM_PROFILE_API = "https://www.showroom-live.com/api/room/profile?room_id={room_id}"

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

# --- 1. イベント経由のID抽出ロジック ---
def get_event_ids(session, status_list=[1, 3, 4]):
    """開催中、予定、終了(1ヶ月以内)のイベントIDを収集"""
    event_ids = set()
    for status in status_list:
        page = 1
        while True:
            try:
                res = session.get(f"{EVENT_SEARCH_API}?status={status}&page={page}", timeout=10)
                res.raise_for_status()
                data = res.json()
                items = data.get("event_list", []) # 構造に合わせて調整
                if not items: break
                for item in items:
                    eid = item.get("event_id")
                    if eid: event_ids.add(str(eid))
                if len(items) < 10: break # 簡易的な最終ページ判定
                page += 1
                if page > 5: break # 負荷軽減のため各ステータス5ページまでに制限（必要に応じ調整）
            except: break
    return list(event_ids)

def get_room_ids_from_event(session, event_id):
    """特定のイベントに参加しているルームIDを全取得"""
    room_ids = set()
    page = 1
    while True:
        try:
            res = session.get(f"{EVENT_ROOM_LIST_API}?event_id={event_id}&p={page}", timeout=10)
            res.raise_for_status()
            data = res.json()
            room_list = data.get("list", [])
            if not room_list: break
            for r in room_list:
                rid = r.get("room_id")
                if rid: room_ids.add(str(rid))
            if not data.get("next_page") or data.get("next_page") <= page: break
            page = data.get("next_page")
            time.sleep(0.2)
        except: break
    return room_ids

# --- 2. ルームステータス取得ロジック ---
def get_room_profile(room_id, session):
    url = ROOM_PROFILE_API.format(room_id=room_id)
    try:
        response = session.get(url, timeout=10)
        response.raise_for_status()
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

def process_status_check(id_list):
    """本体処理: IDリストからB-5以上を抽出して表示"""
    if not id_list:
        st.warning("処理対象のルームIDがありません。")
        return

    all_results = {}
    error_log = {}
    session = create_session()
    
    st.info(f"合計 {len(id_list)} 件のルームを精査します...")
    progress_bar = st.progress(0)
    status_text = st.empty()

    with ThreadPoolExecutor(max_workers=40) as executor:
        futures = {executor.submit(get_room_profile, rid, session): rid for rid in id_list}
        for i, future in enumerate(as_completed(futures)):
            rid, res, msg = future.result()
            if res: all_results[rid] = res
            else: error_log[rid] = msg
            
            if i % 50 == 0 or i == len(id_list)-1:
                progress = (i + 1) / len(id_list)
                progress_bar.progress(progress)
                status_text.text(f"進捗: {i+1} / {len(id_list)}")
    
    progress_bar.empty()
    status_text.empty()
    display_results(all_results, error_log)

def display_results(all_room_data, error_log):
    # CSS
    st.markdown("""<style>
        .result-table { width: 100%; border-collapse: collapse; font-size: 14px; }
        .result-table th { background-color: #f0f2f6; position: sticky; top: 0; padding: 10px; border: 1px solid #ddd; }
        .result-table td { padding: 8px; border: 1px solid #ddd; text-align: center; }
        .hl-up { background-color: #e3f2fd; }
        .hl-low { background-color: #fff9c4; }
    </style>""", unsafe_allow_html=True)

    processed = []
    low_rank_count = 0
    for rid, p in all_room_data.items():
        rank = _safe_get(p, ["show_rank_subdivided"], "-")
        if rank in RANK_ORDER:
            processed.append({
                "rid": rid, "p": p, "rank_idx": RANK_ORDER.index(rank),
                "next": int(_safe_get(p, ["next_score"], 99999999))
            })
        else: low_rank_count += 1

    processed.sort(key=lambda x: (x["rank_idx"], x["next"]))

    st.success(f"【完了】 B-5以上: {len(processed)}件 / ランク外: {low_rank_count}件 / 失敗: {len(error_log)}件")
    
    if processed:
        headers = ["ルーム名", "レベル", "SHOWランク", "上位まで", "下位まで", "フォロワー", "継続日数", "ジャンル"]
        rows = []
        csv_rows = []
        for item in processed:
            p = item["p"]
            rid = item["rid"]
            name = _safe_get(p, ["room_name"], "Unknown")
            rank = _safe_get(p, ["show_rank_subdivided"], "-")
            n_score = _safe_get(p, ["next_score"], "-")
            p_score = _safe_get(p, ["prev_score"], "-")
            
            url = f"https://www.showroom-live.com/room/profile?room_id={rid}"
            rows.append(f"""<tr>
                <td><a href='{url}' target='_blank'>{name}</a></td>
                <td>{_safe_get(p,['room_level'])}</td><td>{rank}</td>
                <td class="{'hl-up' if str(n_score).isdigit() and int(n_score)<=30000 else ''}">{n_score}</td>
                <td class="{'hl-low' if str(p_score).isdigit() and int(p_score)<=30000 else ''}">{p_score}</td>
                <td>{_safe_get(p,['follower_num'])}</td><td>{_safe_get(p,['live_continuous_days'])}</td>
                <td>{GENRE_MAP.get(_safe_get(p,['genre_id']), '-')}</td>
            </tr>""")
            csv_rows.append([name, _safe_get(p,['room_level']), rank, n_score, p_score, _safe_get(p,['follower_num']), _safe_get(p,['live_continuous_days']), GENRE_MAP.get(_safe_get(p,['genre_id']))])

        st.markdown(f'<table class="result-table"><thead>{"".join(f"<th>{h}</th>" for h in headers)}</thead><tbody>{"".join(rows)}</tbody></table>', unsafe_allow_html=True)
        
        df_csv = pd.DataFrame(csv_rows, columns=headers)
        st.download_button("📥 結果をCSV保存", df_csv.to_csv(index=False).encode('utf-8-sig'), "showroom_results.csv", "text/csv")

# --- メイン UI ---
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

tab1, tab2 = st.tabs(["イベントから自動抽出", "手動ID入力"])

with tab1:
    st.markdown("現在および直近1ヶ月のイベント参加ルームを自動的にスキャンします。")
    if st.button("🚀 自動スキャン＆チェック開始"):
        session = create_session()
        with st.spinner("ステップ1: 対象イベントを検索中..."):
            event_ids = get_event_ids(session)
        
        all_event_room_ids = set()
        progress_evt = st.progress(0)
        with st.spinner(f"ステップ2: {len(event_ids)} 個のイベントからルームIDを抽出中..."):
            for i, eid in enumerate(event_ids):
                rids = get_room_ids_from_event(session, eid)
                all_event_room_ids.update(rids)
                progress_evt.progress((i + 1) / len(event_ids))
        
        st.write(f"抽出されたユニークルーム数: {len(all_event_room_ids)} 件")
        process_status_check(list(all_event_room_ids))

with tab2:
    room_ids_raw = st.text_area("ルームIDを入力（改行/カンマ区切り）:", height=200)
    if st.button("🔍 指定IDのステータス表示"):
        id_list = [rid.strip() for rid in re.split(r'[,\s\n]+', room_ids_raw) if rid.strip().isdigit()]
        process_status_check(id_list)