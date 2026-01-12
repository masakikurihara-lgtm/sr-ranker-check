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

# Streamlit の初期設定
st.set_page_config(
    page_title="SHOWROOM ルームステータス確認ツール（高精度版）",
    layout="wide"
)

# --- 定数設定 ---
ROOM_LIST_URL = "https://mksoul-pro.com/showroom/file/room_list.csv"
ROOM_PROFILE_API = "https://www.showroom-live.com/api/room/profile?room_id={room_id}"

GENRE_MAP = {
    112: "ミュージック", 102: "アイドル", 103: "タレント", 104: "声優",
    105: "芸人", 107: "バーチャル", 108: "モデル", 109: "俳優",
    110: "アナウンサー", 113: "クリエイター", 200: "ライバー",
}

RANK_ORDER = [
    "SS-5", "SS-4", "SS-3", "SS-2", "SS-1",
    "S-5", "S-4", "S-3", "S-2", "S-1",
    "A-5", "A-4", "A-3", "A-2", "A-1",
    "B-5"
]

# --- 通信セッションの設定（リトライ機能付き） ---
def create_session():
    session = requests.Session()
    # 500, 502, 503, 504 エラー時に自動リトライ
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session

def get_room_profile(room_id, session):
    """ライバー（ルーム）プロフィール情報APIからデータを取得する"""
    url = ROOM_PROFILE_API.format(room_id=room_id)
    try:
        # User-Agentを設定してブラウザからのアクセスを装う（ブロック回避）
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        response = session.get(url, timeout=10, headers=headers)
        response.raise_for_status()
        data = response.json()
        if not data:
            return room_id, None, "空データ"
        return room_id, data, "成功"
    except Exception as e:
        return room_id, None, str(e)

def display_multiple_room_status(all_room_data, error_log):
    """取得した複数のルームデータを一覧表示"""
    now_str = datetime.datetime.now(JST).strftime('%Y/%m/%d %H:%M:%S')
    st.caption(f"（取得時刻: {now_str} 現在）")
    
    # CSS（省略せず保持）
    st.markdown("""
    <style>
    .basic-info-table-wrapper { width: 100%; overflow-x: auto; }
    .basic-info-table { border-collapse: collapse; width: 100%; margin-top: 10px; }
    .basic-info-table th { text-align: center !important; background-color: #e8eaf6; color: #1a237e; font-weight: bold; padding: 8px 10px; border: 1px solid #c5cae9; white-space: nowrap; }
    .basic-info-table td { text-align: center !important; padding: 8px 10px; border: 1px solid #f0f0f0; white-space: nowrap; font-weight: 600; }
    .basic-info-highlight-upper { background-color: #e3f2fd !important; color: #0d47a1; }
    .basic-info-highlight-lower { background-color: #fff9c4 !important; color: #795548; }
    .room-link { text-decoration: underline; color: #1f2937; }
    </style>
    """, unsafe_allow_html=True)

    headers = ["ルーム名", "ルームレベル", "現在のSHOWランク", "上位ランクまでのスコア", "下位ランクまでのスコア", "フォロワー数", "まいにち配信", "ジャンル", "公式 or フリー"]

    processed_list = []
    low_rank_count = 0

    for room_id, profile_data in all_room_data.items():
        if not profile_data: continue
        show_rank = _safe_get(profile_data, ["show_rank_subdivided"], "-")
        
        if show_rank in RANK_ORDER:
            rank_index = RANK_ORDER.index(show_rank)
            next_score = _safe_get(profile_data, ["next_score"], 0)
            try: next_score_int = int(next_score)
            except: next_score_int = 999999999
            
            processed_list.append({
                "room_id": room_id, "profile_data": profile_data,
                "rank_index": rank_index, "next_score_int": next_score_int
            })
        else:
            low_rank_count += 1

    processed_list.sort(key=lambda x: (x["rank_index"], x["next_score_int"]))

    # データ表示・ダウンロード（中略：ロジックは前回踏襲）
    rows_html = []
    csv_data = []
    for item in processed_list:
        p = item["profile_data"]
        rid = item["room_id"]
        # 各種データ抽出（_safe_get利用）
        name = _safe_get(p, ["room_name"], "取得失敗")
        rank = _safe_get(p, ["show_rank_subdivided"], "-")
        # ... (以下、表示用の整形処理)
        row_url = f"https://www.showroom-live.com/room/profile?room_id={rid}"
        name_html = f'<a href="{row_url}" target="_blank" class="room-link">{name}</a>'
        
        rows_html.append(f"<tr><td>{name_html}</td><td>{p.get('room_level','-')}</td><td>{rank}</td><td>{p.get('next_score','-')}</td><td>{p.get('prev_score','-')}</td><td>{p.get('follower_num','-')}</td><td>{p.get('live_continuous_days','-')}</td><td>{GENRE_MAP.get(p.get('genre_id'),'-')}</td><td>{'公式' if p.get('is_official') else 'フリー'}</td></tr>")
        csv_data.append([name, p.get('room_level'), rank, p.get('next_score'), p.get('prev_score'), p.get('follower_num'), p.get('live_continuous_days'), GENRE_MAP.get(p.get('genre_id')), '公式' if p.get('is_official') else 'フリー'])

    # --- サマリー表示 ---
    st.info(f"【処理結果】 抽出対象(B-5以上): {len(processed_list)}件 / ランク外: {low_rank_count}件 / 取得失敗: {len(error_log)}件")
    
    if error_log:
        with st.expander("取得失敗したIDの確認"):
            st.write(error_log)

    if rows_html:
        st.markdown(f'<div class="basic-info-table-wrapper"><table class="basic-info-table"><thead><tr>{"".join(f"<th>{h}</th>" for h in headers)}</tr></thead><tbody>{"".join(rows_html)}</tbody></table></div>', unsafe_allow_html=True)
        # ダウンロードボタン
        df_download = pd.DataFrame(csv_data, columns=headers)
        st.download_button("📥 CSVをダウンロード", df_download.to_csv(index=False).encode('utf-8-sig'), f"showroom_{datetime.datetime.now().strftime('%Y%m%d')}.csv", "text/csv")

def _safe_get(data, keys, default_value=None):
    temp = data
    for key in keys:
        if isinstance(temp, dict) and key in temp: temp = temp.get(key)
        else: return default_value
    return temp if temp not in [None, "", " "] else default_value

# --- メインロジック ---
if 'authenticated' not in st.session_state: st.session_state.authenticated = False

# 認証部分は前回と同様
if not st.session_state.authenticated:
    st.title("💖 SHOWROOM ステータス確認ツール")
    auth_code = st.text_input("認証コード:", type="password")
    if st.button("ログイン"):
        try:
            res = requests.get(ROOM_LIST_URL)
            if auth_code in res.text:
                st.session_state.authenticated = True
                st.rerun()
        except: st.error("認証エラー")
    st.stop()

if st.session_state.authenticated:
    room_ids_raw = st.text_area("ルームIDを入力（数千件対応）:", height=200)
    if st.button("データ取得開始"):
        id_list = [rid.strip() for rid in re.split(r'[,\s\n]+', room_ids_raw) if rid.strip().isdigit()]
        
        if id_list:
            all_results = {}
            error_log = {}
            session = create_session()
            progress_bar = st.progress(0)
            
            with st.spinner(f"全 {len(id_list)} 件を精査中..."):
                # 6000件超えの場合、サーバー負荷を考慮し同時接続数を少し下げて安定性を重視(40程度)
                with ThreadPoolExecutor(max_workers=40) as executor:
                    future_to_id = {executor.submit(get_room_profile, rid, session): rid for rid in id_list}
                    
                    for i, future in enumerate(as_completed(future_to_id)):
                        rid, res, msg = future.result()
                        if res:
                            all_results[rid] = res
                        else:
                            error_log[rid] = msg
                        
                        if i % 50 == 0:
                            progress_bar.progress((i + 1) / len(id_list))
            
            progress_bar.empty()
            display_multiple_room_status(all_results, error_log)