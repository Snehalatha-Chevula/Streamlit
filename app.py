import streamlit as st
import pandas as pd
import json
from datetime import datetime
from google.oauth2 import id_token
from google.auth.transport import requests as google_requests
from modules.llm_engine import LLMEngine
from modules.database import DatabaseEngine
from modules.data_processor import DataProcessor
from modules.visualization import VisualizationEngine
from modules.prompt_engineering import PromptTemplate
import firebase_admin
from firebase_admin import credentials, auth, firestore
import requests
from urllib.parse import urlencode

if not firebase_admin._apps:
    cred = credentials.Certificate("firebase_admin_sdk.json")
    firebase_admin.initialize_app(cred)

db = firestore.client()

def get_google_login_url():
    params = {
        "client_id": st.secrets["GOOGLE_CLIENT_ID"],
        "redirect_uri": st.secrets["GOOGLE_REDIRECT_URI"],
        "response_type": "code",
        "scope": "openid email profile",
        "access_type": "offline",
        "prompt": "consent"
    }
    return "https://accounts.google.com/o/oauth2/v2/auth?" + urlencode(params)


def exchange_code_for_user(code):
    token_url = "https://oauth2.googleapis.com/token"

    data = {
        "code": code,
        "client_id": st.secrets["GOOGLE_CLIENT_ID"],
        "client_secret": st.secrets["GOOGLE_CLIENT_SECRET"],
        "redirect_uri": st.secrets["GOOGLE_REDIRECT_URI"],
        "grant_type": "authorization_code"
    }

    token_response = requests.post(token_url, data=data)
    token_response.raise_for_status()
    tokens = token_response.json()

    google_id_token = tokens["id_token"]

    #VERIFY USING GOOGLE
    decoded_google_user = id_token.verify_oauth2_token(
        google_id_token,
        google_requests.Request(),
        st.secrets["GOOGLE_CLIENT_ID"]
    )

    uid = decoded_google_user["sub"]

    #CREATE FIREBASE USER IF NOT EXISTS
    try:
        user = auth.get_user(uid)
    except auth.UserNotFoundError:
        user = auth.create_user(
            uid=uid,
            email=decoded_google_user.get("email"),
            display_name=decoded_google_user.get("name"),
            photo_url=decoded_google_user.get("picture")
        )

    return {
        "uid": user.uid,
        "email": user.email,
        "name": user.display_name,
        "picture": user.photo_url
    }


def save_query_to_firebase(user_id, query_data):
    """
    Save a single query record to Firestore for a user
    """
    db.collection("query_history") \
      .document(user_id) \
      .collection("queries") \
      .add(query_data)

def load_initial_query_history(user_id, limit=5):
    """
    Load the latest `limit` queries for a user (initial page)
    """
    query_ref = (
        db.collection("query_history")
          .document(user_id)
          .collection("queries")
          .order_by("timestamp", direction=firestore.Query.DESCENDING)
          .limit(limit)
    )

    docs = list(query_ref.stream())

    history = []
    for doc in docs:
        data = doc.to_dict()
        history.append({
            "natural_query": data.get("natural_query"),
            "sql": data.get("sql"),
            "chart_type": data.get("chart_type"),
            "timestamp": data.get("timestamp")
        })

    #last document
    if docs:
        st.session_state.last_query_doc = docs[-1]

    return history

def load_more_query_history(user_id, limit=5):
    """
    Load older queries using pagination cursor
    """
    if not st.session_state.last_query_doc:
        return []

    query_ref = (
        db.collection("query_history")
          .document(user_id)
          .collection("queries")
          .order_by("timestamp", direction=firestore.Query.DESCENDING)
          .start_after(st.session_state.last_query_doc)
          .limit(limit)
    )

    docs = list(query_ref.stream())

    history = []
    for doc in docs:
        data = doc.to_dict()
        history.append({
            "natural_query": data.get("natural_query"),
            "sql": data.get("sql"),
            "chart_type": data.get("chart_type"),
            "timestamp": data.get("timestamp")
        })

    #Update cursor
    if docs:
        st.session_state.last_query_doc = docs[-1]

    return history

def clear_user_query_history(user_id):
    """
    Delete all query history for a user from Firestore
    """
    queries_ref = (
        db.collection("query_history")
          .document(user_id)
          .collection("queries")
    )

    docs = queries_ref.stream()

    batch = db.batch()
    count = 0

    for doc in docs:
        batch.delete(doc.reference)
        count += 1

        if count % 400 == 0:
            batch.commit()
            batch = db.batch()

    batch.commit()

def clear_history():
    if st.session_state.user:
        clear_user_query_history(st.session_state.user["uid"])
        st.session_state.last_query_doc = None

    st.session_state.query_history = []
    st.rerun()


def render_auth_ui():
    firebase_api_key = st.secrets["FIREBASE_API_KEY"]
    firebase_auth_domain = st.secrets["FIREBASE_AUTH_DOMAIN"]
    firebase_project_id = st.secrets["FIREBASE_PROJECT_ID"]
    firebase_app_id = st.secrets["FIREBASE_APP_ID"]

    auth_html = f"""
    <script src="https://www.gstatic.com/firebasejs/9.23.0/firebase-app-compat.js"></script>
    <script src="https://www.gstatic.com/firebasejs/9.23.0/firebase-auth-compat.js"></script>

    <script>
        const firebaseConfig = {{
            apiKey: "{firebase_api_key}",
            authDomain: "{firebase_auth_domain}",
            projectId: "{firebase_project_id}",
            appId: "{firebase_app_id}"
        }};

        if (!firebase.apps.length) {{
            firebase.initializeApp(firebaseConfig);
        }}

        const auth = firebase.auth();
        const provider = new firebase.auth.GoogleAuthProvider();

        function signIn() {{
            auth.signInWithRedirect(provider);
        }}

        auth.getRedirectResult().then(result => {{
            if (result.user) {{
                result.user.getIdToken().then(token => {{
                    const url = new URL(window.location);
                    url.searchParams.set("token", token);
                    window.location.href = url.toString();
                }});
            }}
        }}).catch(error => {{
            console.error(error);
            alert(error.message);
        }});

        function signOut() {{
            auth.signOut().then(() => {{
                window.location.href = window.location.pathname;
            }});
        }}
    </script>

    <div style="margin-bottom:20px;">
        <button onclick="signIn()" style="
            width:100%;
            padding:10px;
            background:#4285F4;
            color:white;
            border:none;
            border-radius:5px;
            font-size:14px;
            cursor:pointer;">
            🔐 Login with Google
        </button>
    </div>
    """

    st.components.v1.html(auth_html, height=140)

#STREAMLIT PAGE CONFIGURATION

st.set_page_config(
    page_title="LLM SQL Query Assistant",
    page_icon="📊",
    layout="wide"
)

# CUSTOM CSS

st.markdown("""
<style>
    button:hover {
        opacity: 0.95;
    }
    .title-section {
        text-align: center;
        padding: 20px;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border-radius: 10px;
        color: white;
        margin-bottom: 30px;
    }
    .result-box {
        background-color: #f0f2f6;
        padding: 15px;
        border-radius: 8px;
        margin: 10px 0;
    }
    .error-box {
        background-color: #ffebee;
        padding: 15px;
        border-radius: 8px;
        color: #c62828;
        margin: 10px 0;
    }
    .success-box {
        background-color: #e8f5e9;
        padding: 15px;
        border-radius: 8px;
        color: #2e7d32;
        margin: 10px 0;
    }
</style>
""", unsafe_allow_html=True)

# INITIALIZE SESSION STATE

if "df" not in st.session_state:
    st.session_state.df = None

if "schema" not in st.session_state:
    st.session_state.schema = None

if "db_engine" not in st.session_state:
    st.session_state.db_engine = None

if "llm_engine" not in st.session_state:
    st.session_state.llm_engine = LLMEngine()

if "query_history" not in st.session_state:
    st.session_state.query_history = []

if "data_processor" not in st.session_state:
    st.session_state.data_processor = DataProcessor()

if "viz_engine" not in st.session_state:
    st.session_state.viz_engine = VisualizationEngine()

if "user" not in st.session_state:
    st.session_state.user = None 

if "last_query_doc" not in st.session_state:
    st.session_state.last_query_doc = None


query_params = st.query_params

if "code" in query_params and st.session_state.user is None:
    try:
        user_info = exchange_code_for_user(query_params["code"])
        st.session_state.user = user_info

        # 🔹 LOAD INITIAL (PAGINATED) QUERY HISTORY
        st.session_state.query_history = load_initial_query_history(
            user_info["uid"]
        )

        st.query_params.clear()
        st.rerun()
    except Exception as e:
        st.error("Authentication failed. Please try again.")

# MAIN TITLE

st.markdown("""
<div class="title-section">
    <h1>📊 LLM-Powered SQL Query Assistant</h1>
    <p>Ask questions in natural language. Let AI convert them to SQL.</p>
</div>
""", unsafe_allow_html=True)

# SIDEBAR CONFIGURATION & SETTINGS


with st.sidebar:
    if st.session_state.user:
        initial = st.session_state.user["name"][0].upper()

        st.components.v1.html(
            f"""
            <div style="
                background:#f6f8fc;
                padding:12px 14px;
                border-radius:10px;
                box-shadow:0 2px 8px rgba(0,0,0,0.06);
                display:flex;
                align-items:center;
                gap:12px;
                margin:0px;   /* 🔹 reduced gap */
                font-family: system-ui, -apple-system, BlinkMacSystemFont;
            ">
                <!-- Avatar -->
                <div style="
                    width:38px;
                    height:38px;
                    min-width:38px;
                    border-radius:50%;
                    background:#667eea;
                    color:white;
                    display:flex;
                    align-items:center;
                    justify-content:center;
                    font-weight:700;
                    font-size:16px;
                    line-height:1;
                ">
                    {initial}
                </div>

                <!-- User info -->
                <div style="line-height:1.3;">
                    <div style="
                        font-weight:600;
                        font-size:12px;
                        color:#2c2c2c;
                    ">
                        {st.session_state.user['name']}
                    </div>
                    <div style="
                        font-size:12px;
                        color:#555;
                        word-break:break-all;
                    ">
                        {st.session_state.user['email']}
                    </div>
                </div>
            </div>
            """,
            height=100
        )

        col1,col2, col3 = st.columns([0.1, 12, 1])
        with col2:
            if st.button("Logout",use_container_width=True):
                st.session_state.user = None
                st.session_state.query_history = None
                st.rerun()

    else:
        login_url = get_google_login_url()
        st.markdown(
            f"""
            <a href="{login_url}" target="_self">
                <button style="
                    width:100%;
                    padding:10px;
                    background:#4285F4;
                    color:white;
                    border:none;
                    border-radius:6px;
                    font-size:14px;
                    cursor:pointer;
                    margin-bottom:15px;
                ">
                    🔐 Login with Google
                </button>
            </a>
            """,
            unsafe_allow_html=True
        )

    # Query History header 
    col_h1, col_h2 = st.columns([0.75, 0.25])

    with col_h1:
        st.markdown("### 📜 Query History")

    with col_h2:
        if st.button("🗑️", help="Clear query history"):
            clear_history()


    #DATA UPLOAD
    if st.session_state.query_history:
        for i, query in enumerate(st.session_state.query_history, 1):
            with st.expander(f"{query['natural_query'][:50]}..."):
                st.code(query["sql"], language="sql")

        # Load more button 
        if st.session_state.user:
            if st.button("⬇️ Load more", use_container_width=True):
                older_queries = load_more_query_history(
                    st.session_state.user["uid"]
                )

                if older_queries:
                    st.session_state.query_history.extend(older_queries)
                    st.rerun()
                else:
                    st.info("No more queries to load")
    else:
        st.info("No queries yet")


col1, _ = st.columns([2,0.5])

with col1:
    st.header("Step 1️⃣ Upload Your Dataset")
    uploaded_file = st.file_uploader(
        "Upload CSV file",
        type=["csv"],
        help="Upload your dataset as a CSV file"
    )


# Process uploaded file
if uploaded_file:
    try:
        df = pd.read_csv(uploaded_file)
        st.session_state.df = df

        # Initialize database and extract schema
        st.session_state.db_engine = DatabaseEngine()
        st.session_state.db_engine.load_dataframe(df, "data")
        st.session_state.schema = st.session_state.db_engine.get_schema()

        # Display data preview
        st.success(f"✓ Dataset loaded: {df.shape[0]} rows, {df.shape[1]} columns")

        with st.expander("👀 Preview Dataset", expanded=True):
            st.dataframe(df.head(10), width="stretch")

        with st.expander("📋 Dataset Schema"):
            schema_df = pd.DataFrame([
                {"Column": col, "Type": str(df[col].dtype)}
                for col in df.columns
            ])
            st.dataframe(schema_df, width="stretch")

    except Exception as e:
        st.error(f"Error loading file: {str(e)}")

#NATURAL LANGUAGE QUERY


if st.session_state.df is not None:
    st.divider()
    st.header("Step 2️⃣ Ask Your Question")

  
    query_text = st.text_area(
        "Enter your question in natural language",
        placeholder="e.g., What is the total sales by region?",
        height=100,
        help="Ask any question about your data"
    )

    col1, col2, col3 = st.columns(3)

    with col1:
        submit_button = st.button("🔍 Generate SQL & Visualize", width='stretch')

    with col2:
        show_sql = st.checkbox("Show SQL Query")

    with col3:
        clear_history = st.button("🗑️ Clear History", width='stretch')

    if clear_history:
        clear_history()

    #PROCESS QUERY & GENERATE SQL

    if submit_button and query_text:
        with st.spinner("🤔 Generating SQL query..."):
            try:
                # Create prompt
                prompt_template = PromptTemplate(st.session_state.schema)
                prompt = prompt_template.create_prompt(query_text)

                # Generate SQL
                llm_response = st.session_state.llm_engine.generate_sql(prompt)

                # Parse response
                response_json = json.loads(llm_response)
                sql_query = response_json.get("sql_query")
                chart_type = response_json.get("chart_type", "table")
                axis_mapping = response_json.get("axis_mapping", {})
                description = response_json.get("description", "")

                # EXECUTE SQL

                with st.spinner("⚙️ Executing query..."):
                    result_df = st.session_state.db_engine.execute_query(sql_query)

                # PROCESS DATA

                with st.spinner("📊 Processing results..."):
                    processed_data = st.session_state.data_processor.process_results(
                        result_df,
                        chart_type,
                        axis_mapping
                    )

                # RENDER VISUALIZATION

                st.success("✓ Query executed successfully!")

                # Add to history
                query_record = {
                    "timestamp": firestore.SERVER_TIMESTAMP,
                    "natural_query": query_text,
                    "sql": sql_query,
                    "chart_type": chart_type
                }

                if st.session_state.user:
                    # 🔹 Logged-in user → save to Firebase
                    save_query_to_firebase(
                        st.session_state.user["uid"],
                        query_record
                    )

                    # update UI
                    st.session_state.query_history.insert(0, query_record)
                else:
                    #Guest user
                    st.session_state.query_history.insert(0, query_record)


                # Display results
                st.divider()
                st.header("Results")

                # Show SQL if requested
                if show_sql:
                    st.subheader("SQL Query Generated")
                    st.code(sql_query, language="sql")

                # Show description
                if description:
                    st.markdown(f"**Analysis:** {description}")

                # Display chart
                st.subheader(f"📈 {chart_type.upper()} Chart")

                fig = st.session_state.viz_engine.render_chart(
                    processed_data,
                    chart_type,
                    axis_mapping
                )

                st.plotly_chart(fig, width='stretch')

                # Display data table
                with st.expander("📋 View Raw Data"):
                    st.dataframe(result_df, width='stretch')

                # Export options
                st.divider()
                col1, col2 = st.columns(2)

                with col1:
                    csv = result_df.to_csv(index=False)
                    st.download_button(
                        "📥 Download as CSV",
                        csv,
                        "results.csv",
                        "text/csv"
                    )

                with col2:
                    json_str = result_df.to_json(orient="records")
                    st.download_button(
                        "📥 Download as JSON",
                        json_str,
                        "results.json",
                        "application/json"
                    )

            except json.JSONDecodeError:
                st.error("❌ Error parsing LLM response. Ensure output is valid JSON.")

            except Exception as e:
                st.warning(
                    "⚠️ No matching data found. "
                    "Try rephrasing the question or check available data in the dataset."
                )

# FOOTER

st.divider()
st.markdown("""
<div style='text-align: center; color: #666; padding: 10px;'>
    <p>LLM SQL Query Assistant | Powered by Streamlit</p>
    <p><small>Built for non-technical users to query data with natural language</small></p>
</div>
""", unsafe_allow_html=True)
