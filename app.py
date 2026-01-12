import streamlit as st
import requests
import pandas as pd
import io
import datetime
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

JST = datetime.timezone(datetime.timedelta(hours=9))

# Streamlit の初期設定
st.set_page_config(
    page_title="SHOWROOM ルームステータス確認ツール（高速一括版）",
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

# --- SHOWランクの定義（順序付け用） ---
RANK_ORDER = [
    "SS-5", "SS-4", "SS-3", "SS-2", "SS-1",
    "S-5", "S-4", "S-3", "S-2", "S-1",
    "A-5", "A-4", "A-3", "A-2", "A-1",
    "B-5"
]

# --- ユーティリティ関数 ---

def _safe_get(data, keys, default_value=None):
    """ネストされた辞書から安全に値を取得するヘルパー関数"""
    temp = data
    for key in keys:
        if isinstance(temp, dict) and key in temp:
            temp = temp.get(key)
        else:
            return default_value
    if temp is None or (isinstance(temp, str) and temp.strip() == "") or (isinstance(temp, float) and pd.isna(temp)):
        return default_value
    return temp

def get_room_profile(room_id):
    """ライバー（ルーム）プロフィール情報APIからデータを取得する"""
    url = ROOM_PROFILE_API.format(room_id=room_id)
    try:
        # タイムアウトを少し短めに設定し、スタックを防ぐ
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        return room_id, response.json()
    except Exception:
        return room_id, None

def display_multiple_room_status(all_room_data):
    """取得した複数のルームデータを一覧表示し、ダウンロード機能を提供する"""

    now_str = datetime.datetime.now(JST).strftime('%Y/%m/%d %H:%M:%S')
    st.caption(f"（取得時刻: {now_str} 現在）")
    
    custom_styles = """
    <style>
    .basic-info-table-wrapper { width: 100%; margin: 0 auto; overflow-x: auto; }
    .basic-info-table { border-collapse: collapse; width: 100%; margin-top: 10px; }
    .basic-info-table th { text-align: center !important; background-color: #e8eaf6; color: #1a237e; font-weight: bold; padding: 8px 10px; border: 1px solid #c5cae9; white-space: nowrap; }
    .basic-info-table td { text-align: center !important; padding: 8px 10px; line-height: 1.4; border: 1px solid #f0f0f0; white-space: nowrap; font-weight: 600; }
    .basic-info-table tbody tr:hover { background-color: #f7f9fd; }
    .basic-info-highlight-upper { background-color: #e3f2fd !important; color: #0d47a1; }
    .basic-info-highlight-lower { background-color: #fff9c4 !important; color: #795548; }
    .room-link { text-decoration: underline; color: #1f2937; }
    </style>
    """
    st.markdown(custom_styles, unsafe_allow_html=True)

    headers = [
        "ルーム名", "ルームレベル", "現在のSHOWランク", "上位ランクまでのスコア", 
        "下位ランクまでのスコア", "フォロワー数", "まいにち配信", "ジャンル", "公式 or フリー"
    ]

    def is_within_30000(value):
        try: return int(value) <= 30000
        except: return False

    def format_value(value):
        if value == "-" or value is None: return "-"
        try: return f"{int(value):,}"
        except: return str(value)

    processed_list = []
    for room_id, profile_data in all_room_data.items():
        if not profile_data:
            continue
            
        show_rank = _safe_get(profile_data, ["show_rank_subdivided"], "-")
        
        if show_rank in RANK_ORDER:
            rank_index = RANK_ORDER.index(show_rank)
            next_score = _safe_get(profile_data, ["next_score"], 0)
            try: next_score_int = int(next_score)
            except: next_score_int = 999999999
            
            processed_list.append({
                "room_id": room_id,
                "profile_data": profile_data,
                "rank_index": rank_index,
                "next_score_int": next_score_int
            })

    # ソート
    processed_list.sort(key=lambda x: (x["rank_index"], x["next_score_int"]))

    rows_html = []
    csv_data = []

    for item in processed_list:
        room_id = item["room_id"]
        profile_data = item["profile_data"]

        room_name = _safe_get(profile_data, ["room_name"], "取得失敗")
        room_level = _safe_get(profile_data, ["room_level"], "-")
        show_rank = _safe_get(profile_data, ["show_rank_subdivided"], "-")
        next_score = _safe_get(profile_data, ["next_score"], "-")
        prev_score = _safe_get(profile_data, ["prev_score"], "-")
        follower_num = _safe_get(profile_data, ["follower_num"], "-")
        live_continuous_days = _safe_get(profile_data, ["live_continuous_days"], "-")
        is_official = _safe_get(profile_data, ["is_official"], None)
        genre_id = _safe_get(profile_data, ["genre_id"], None)

        official_status = "公式" if is_official is True else "フリー" if is_official is False else "-"
        genre_name = GENRE_MAP.get(genre_id, f"その他 ({genre_id})" if genre_id else "-")
        room_url = f"https://www.showroom-live.com/room/profile?room_id={room_id}"
        
        room_name_cell = f'<a href="{room_url}" target="_blank" class="room-link">{room_name}</a>'
        display_values = [
            room_name_cell, format_value(room_level), show_rank, format_value(next_score), 
            format_value(prev_score), format_value(follower_num), format_value(live_continuous_days), 
            genre_name, official_status
        ]

        td_html = []
        for i, value in enumerate(display_values):
            header_name = headers[i]
            css_class = ""
            if header_name == "上位ランクまでのスコア" and is_within_30000(next_score):
                css_class = "basic-info-highlight-upper"
            elif header_name == "下位ランクまでのスコア" and is_within_30000(prev_score):
                css_class = "basic-info-highlight-lower"
            td_html.append(f'<td class="{css_class}">{value}</td>')
        
        rows_html.append(f"<tr>{''.join(td_html)}</tr>")
        csv_data.append([room_name, room_level, show_rank, next_score, prev_score, follower_num, live_continuous_days, genre_name, official_status])

    if not rows_html:
        st.info("B-5以上の条件に一致するルームが見つかりませんでした。")
        return

    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown(f"<h1 style='font-size:22px; text-align:left; color:#1f2937; padding: 15px 0px 5px 0px;'>📊 ルーム基本情報一覧 ({len(processed_list)}件ヒット)</h1>", unsafe_allow_html=True)
    
    with col2:
        if csv_data:
            df_download = pd.DataFrame(csv_data, columns=headers)
            csv = df_download.to_csv(index=False).encode('utf-8-sig')
            st.download_button(
                label="📥 CSVをダウンロード",
                data=csv,
                file_name=f"showroom_status_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
            )

    html_content = f"""
    <div class="basic-info-table-wrapper">
        <table class="basic-info-table">
            <thead><tr>{"".join(f'<th>{h}</th>' for h in headers)}</tr></thead>
            <tbody>{"".join(rows_html)}</tbody>
        </table>
    </div>
    """
    st.markdown(html_content, unsafe_allow_html=True)

# --- メインロジック ---
if 'authenticated' not in st.session_state: st.session_state.authenticated = False
if 'show_status' not in st.session_state: st.session_state.show_status = False
if 'input_room_ids' not in st.session_state: st.session_state.input_room_ids = ""

if not st.session_state.authenticated:
    st.markdown("<h1 style='font-size:28px; text-align:left; color:#1f2937;'>💖 SHOWROOM ルームステータス確認ツール</h1>", unsafe_allow_html=True)
    input_auth_code = st.text_input("認証コードを入力してください:", type="password")
    if st.button("認証する"):
        try:
            response = requests.get(ROOM_LIST_URL, timeout=5)
            room_df = pd.read_csv(io.StringIO(response.text), header=None, dtype=str)
            if input_auth_code.strip() in set(room_df.iloc[:, 0].dropna()):
                st.session_state.authenticated = True
                st.rerun()
            else: st.error("❌ 認証コードが無効です。")
        except Exception as e: st.error(f"接続エラー: {e}")
    st.stop()

if st.session_state.authenticated:
    st.markdown("##### 🔎 ルームIDの入力（大量データ対応版）")
    input_text = st.text_area("ルームIDを入力:", value=st.session_state.input_room_ids, height=200).strip()
    
    if input_text != st.session_state.input_room_ids:
        st.session_state.input_room_ids = input_text
        st.session_state.show_status = False
        
    if st.button("ルームステータスを表示"):
        if st.session_state.input_room_ids:
            st.session_state.show_status = True
        else: st.warning("ルームIDを入力してください。")
            
    if st.session_state.show_status and st.session_state.input_room_ids:
        id_list = [rid.strip() for rid in re.split(r'[,\s\n]+', st.session_state.input_room_ids) if rid.strip().isdigit()]
        
        if not id_list:
            st.error("有効なルームIDが見つかりませんでした。")
        else:
            all_results = {}
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            # --- 並列処理の実装 (ThreadPoolExecutor) ---
            # 6000件超えのため、スレッド数は多めの50程度に設定
            with st.spinner(f"{len(id_list)} 件の情報を取得中..."):
                with ThreadPoolExecutor(max_workers=50) as executor:
                    future_to_id = {executor.submit(get_room_profile, rid): rid for rid in id_list}
                    
                    for i, future in enumerate(as_completed(future_to_id)):
                        rid, res = future.result()
                        if res:
                            all_results[rid] = res
                        
                        # 進捗表示（100件ごとに更新して負荷を軽減）
                        if i % 100 == 0 or i == len(id_list) - 1:
                            progress = (i + 1) / len(id_list)
                            progress_bar.progress(progress)
                            status_text.text(f"取得中: {i+1} / {len(id_list)}")

            progress_bar.empty()
            status_text.empty()
            display_multiple_room_status(all_results)