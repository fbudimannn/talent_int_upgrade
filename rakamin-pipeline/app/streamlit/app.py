import streamlit as st
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account
import openai
import plotly.express as px
import plotly.graph_objects as go

# --- PAGE CONFIGURATION ---
st.set_page_config(layout="wide", page_title="Talent Match App (BigQuery Version)")

# Load Environment Variables
load_dotenv()

# Force Streamlit to read secrets from the project directory
os.environ["STREAMLIT_CONFIG_DIR"] = os.path.join(os.getcwd(), ".streamlit")

# ==============================================================================
# 1. CONNECT KE BIGQUERY
# ==============================================================================
@st.cache_resource
def get_bq_client():
    """
    Works for BOTH:
    - Local development (load key from .env path)
    - Streamlit Cloud (load key from st.secrets, write to /tmp)
    """
    try:
        import json
        
        # ============== 1️⃣ DETECT STREAMLIT CLOUD ==============
        running_in_cloud = st.secrets.get("GCP_SERVICE_ACCOUNT_JSON") is not None

        if running_in_cloud:
            # STREAMLIT CLOUD MODE
            service_json = st.secrets["GCP_SERVICE_ACCOUNT_JSON"]

            key_path = "/tmp/gcp-key.json"
            with open(key_path, "w") as f:
                f.write(service_json)

            credentials = service_account.Credentials.from_service_account_file(key_path)
            project_id = st.secrets["BIGQUERY_PROJECT_ID"]

        else:
            # LOCAL MODE
            key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            project_id = os.getenv("BIGQUERY_PROJECT_ID")

            if not os.path.exists(key_path):
                st.error(f"Local GCP key not found at: {key_path}")
                return None

            credentials = service_account.Credentials.from_service_account_file(key_path)

        # ============== CONNECT CLIENT ==============
        client = bigquery.Client(credentials=credentials, project=project_id)
        return client

    except Exception as e:
        st.error(f"Gagal koneksi ke BigQuery: {e}")
        return None
        
client = get_bq_client()

# ==============================================================================
# 2. DEF GET EMPLOYEE LIST
# ==============================================================================
@st.cache_data
def get_employee_list():
    """
    Mengambil daftar semua karyawan beserta rating terakhir.
    """
    if not client: return pd.DataFrame()

    dataset_prod = os.getenv('BIGQUERY_STAGING_DATASET', 'dbt_prod')
    dataset_raw = os.getenv('BIGQUERY_DIM_DATASET', 'raw_data')
    project_id = os.getenv('BIGQUERY_PROJECT_ID')

    query = f"""
    SELECT 
        e.employee_id, 
        e.fullname, 
        e.position as role, 
        e.grade,
        -- Ambil rating 2025, jika null isi 0
        COALESCE(py.rating, 0) as rating
    FROM `{project_id}.{dataset_prod}.stg_employees` e
    LEFT JOIN `{project_id}.{dataset_raw}.performance_yearly` py 
        ON e.employee_id = py.employee_id 
        AND py.year = 2025
    ORDER BY e.fullname
    """
    
    df = client.query(query).to_dataframe()
    
    # Create display label
    df['display'] = df['fullname'] + " (" + df['employee_id'] + ")"
    
    # Handle NULLs & Data Types
    df['role'] = df['role'].fillna('Unknown')
    df['grade'] = df['grade'].fillna('Unknown')
    df['rating'] = df['rating'].astype(float) # Pastikan rating jadi float biar aman
    
    return df

# ==============================================================================
# 3. DEF GENERATED PROFILE (AI)
# ==============================================================================
@st.cache_data
def generate_job_profile(role_name, job_level, role_purpose, df_results):
    """
    Generates AI Profile using OpenRouter/OpenAI.
    """
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        return "Error: Missing OpenRouter API key."

    try:
        df_baseline = df_results[['tv_name', 'baseline_score']].drop_duplicates()
        
        profile_dict = {}
        for _, row in df_baseline.iterrows():
            val = row['baseline_score']
            profile_dict[f"baseline_{row['tv_name'].lower()}"] = val

        competency_fields = ['sea', 'qdd', 'ftc', 'ids', 'vcu', 'sto_lie', 'csi', 'cex_gdr']
        pillar_labels = {
            'GDR': 'Growth Drive & Resilience', 'CEX': 'Curiosity & Experimentation',
            'IDS': 'Insight & Decision Sharpness', 'QDD': 'Quality Delivery Discipline',
            'STO': 'Synergy & Team Orientation', 'SEA': 'Social Empathy & Awareness',
            'VCU': 'Value Creation for Users', 'LIE': 'Lead, Inspire & Empower',
            'FTC': 'Forward Thinking & Clarity', 'CSI': 'Commercial Savvy & Impact'
        }

        competencies_list = []
        for field in competency_fields:
            key = f"baseline_{field}"
            val = profile_dict.get(key, 0)
            try:
                num_val = float(val)
                if num_val >= 4.5:
                    comp_code = field.upper()
                    label = pillar_labels.get(comp_code.split('_')[0], comp_code)
                    competencies_list.append(f"{label} ({num_val})")
            except:
                pass

        if not competencies_list:
             competencies_list = ["Standard Competencies based on Benchmark Median"]

        competency_desc = "\n".join([f"- {c}" for c in competencies_list])

        profile_details = f"""
        - Ideal Education: {profile_dict.get('baseline_education', 'N/A')}
        - Ideal Major: {profile_dict.get('baseline_major', 'N/A')}
        - Ideal Position: {profile_dict.get('baseline_position', 'N/A')}
        - Avg Tenure: {profile_dict.get('baseline_tenure', 'N/A')} months
        - Key Competencies (Top Performers):\n{competency_desc}
        - Dominant MBTI: {profile_dict.get('baseline_mbti', 'N/A')}
        - Dominant DISC: {profile_dict.get('baseline_disc', 'N/A')}
        """
        
    except Exception as e:
        return f"Error preparing data for AI: {e}"

    prompt = f"""
    You are an expert HR strategist.
    
    ### DATA (Source of Truth from Top Performers):
    {profile_details}
    
    ### CONTEXT:
    Role: {role_name}
    Level: {job_level}
    Purpose: {role_purpose}
    
    ### TASK:
    Create a strategic ideal job profile.
    1. Key Requirements (Bullet points, combine Data + Context)
    2. Key Competencies (Bullet points, explain why based on Data + Context)
    
    Keep it professional and concise.
    """

    try:
        client_ai = openai.OpenAI(base_url="https://openrouter.ai/api/v1", api_key=api_key)
        response = client_ai.chat.completions.create(
            model="meta-llama/llama-3.3-70b-instruct:free", 
            messages=[
                {"role": "system", "content": "You are an HR expert."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=1000, 
            temperature=0.7 
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        return f"Failed to generate AI profile: {e}"

# ==============================================================================
# 4. FUNGSI EKSEKUSI SQL UTAMA  — FIXED VERSION ⭐
# ==============================================================================
def run_benchmark_analysis(selected_ids):
    if not selected_ids:
        return pd.DataFrame()

    # -- Format employee IDs menjadi string untuk SQL
    formatted_ids = ", ".join([f"'{x}'" for x in selected_ids])

    try:
        # ======================================================
        # 🔥 FIX: Cari file SQL berdasarkan lokasi file app.py
        # ======================================================
        base_dir = os.path.dirname(os.path.abspath(__file__))
        sql_path = os.path.join(base_dir, "query_benchmark.sql")

        if not os.path.exists(sql_path):
            st.error(f"❌ File SQL tidak ditemukan di path:\n{sql_path}")
            st.error("Pastikan file 'query_benchmark.sql' berada di folder yang sama dengan app.py")
            return pd.DataFrame()

        # Baca template SQL
        with open(sql_path, "r", encoding="utf-8") as f:
            query_template = f.read()

        # Render final query
        project_id = os.getenv('BIGQUERY_PROJECT_ID')
        dataset_id = os.getenv('BIGQUERY_STAGING_DATASET', 'dbt_prod')

        final_query = query_template.format(
            benchmark_ids_list=formatted_ids,
            project=project_id,
            dataset=dataset_id
        )

        # Eksekusi query
        df = client.query(final_query).to_dataframe()
        return df

    except Exception as e:
        st.error(f"🔥 ERROR SQL EXECUTION:\n{str(e)}")
        return pd.DataFrame()


# ==============================================================================
# 5. STREAMLIT UI (SIDEBAR & CASCADING FILTERS)
# ==============================================================================
st.title("Talent Match Intelligence System 🧠✨")
st.markdown("Use the sidebar to define your new role and select benchmark employees.")

with st.sidebar:
    st.header("Vacancy & Benchmark Settings")
    
    # --- 1. Define New Role ---
    st.subheader("1. Define New Role")
    role_name_input = st.text_input("New Role Name", "Data Analyst") 
    job_level_input = st.selectbox("New Job Level", ["Staff", "Supervisor", "Manager", "Senior Manager"], index=1)
    
    role_purpose_input = st.text_area(
        "Role Purpose", 
        "Describe the main business objective... (e.g., 'To build a new data science team')", 
        height=100
    )

    # Ambil Semua Karyawan (Data Mentah)
    df_master = get_employee_list()

    # --- 2. Find Benchmarks (CASCADING FILTER) ---
    st.subheader("2. Find Benchmarks (Filters)")

    if not df_master.empty:
        # A. FILTER ROLE
        all_roles = sorted(df_master['role'].unique())
        filter_role = st.selectbox("Filter by Role:", ["All"] + all_roles)
        
        # Apply Filter Role
        df_step1 = df_master.copy()
        if filter_role != "All":
            df_step1 = df_step1[df_step1['role'] == filter_role]
        
        # B. FILTER GRADE (Opsi ngikut step 1)
        available_grades = sorted(df_step1['grade'].unique())
        filter_grade = st.selectbox("Filter by Grade:", ["All"] + available_grades)
        
        # Apply Filter Grade
        df_step2 = df_step1.copy()
        if filter_grade != "All":
            df_step2 = df_step2[df_step2['grade'] == filter_grade]
            
        # C. FILTER RATING (Opsi ngikut step 2)
        available_ratings = sorted([r for r in df_step2['rating'].unique() if r > 0], reverse=True)
        available_ratings_str = [str(r) for r in available_ratings]
        
        filter_rating = st.selectbox("Filter by Rating (2025):", ["All"] + available_ratings_str)
        
        # Apply Filter Rating
        # JIKA "All", jangan filter (ambil semua rating 1-5). Jika angka, baru filter.
        filtered_df = df_step2.copy()
        if filter_rating != "All":
            sel_rating = float(filter_rating)
            filtered_df = filtered_df[filtered_df['rating'] == sel_rating]

    else:
        st.warning("Gagal mengambil data karyawan.")
        filtered_df = pd.DataFrame(columns=['display', 'employee_id'])

    # --- 3. Select Benchmark Employees ---
    st.subheader("3. Select Benchmark Employees")
    
    st.caption(f"Candidates found: {len(filtered_df)}")
    
    # Default Selection Logic
    default_ids = ['EMP100012','EMP100524','EMP100548']
    valid_defaults = []
    if not filtered_df.empty:
        valid_defaults = filtered_df[filtered_df['employee_id'].isin(default_ids)]['display'].tolist()
    
    selected_benchmarks = st.multiselect(
        f"Select Benchmarks (Max 3):",
        options=filtered_df['display'],
        max_selections=3,
        default=valid_defaults[:3] 
    )

    selected_benchmark_ids = [x.split('(')[-1].replace(')', '') for x in selected_benchmarks]

    generate_button = st.button("✨ Generate Profile & Matches")


# ==============================================================================
# 6. MAIN LOGIC EXECUTION
# ==============================================================================
if generate_button:
    if not selected_benchmark_ids:
        st.sidebar.error("Please select at least one benchmark employee.")
    else:
        with st.spinner("Analyzing talent data via BigQuery... ⏳"):
            df_sql_results = run_benchmark_analysis(selected_benchmark_ids)
            
            if not df_sql_results.empty:
                st.session_state.sql_results = df_sql_results
                st.session_state.inputs = {
                    'role': role_name_input, 
                    'level': job_level_input, 
                    'purpose': role_purpose_input,
                    'benchmarks': selected_benchmark_ids
                }
                st.success("Analysis complete! Results below. 👇")
            else:
                st.error("Query executed but returned no results.")

# ==============================================================================
# 7. DISPLAY RESULTS (DASHBOARD)
# ==============================================================================
if 'sql_results' in st.session_state:
    df = st.session_state.sql_results
    inputs = st.session_state.inputs
    employee_list_df = get_employee_list() 

    try:
        # --- A. AI JOB PROFILE ---
        st.write("---") 
        st.subheader("🤖 Data-Driven Job Profile") 
        
        ai_profile = generate_job_profile(
            inputs['role'], 
            inputs['level'], 
            inputs['purpose'], 
            df
        )
        st.markdown(ai_profile)

        # --- B. RANKED TALENT LIST ---
        st.write("---") 
        st.subheader("📊 Ranked Talent List")
        
        # 1. Aggregasi Data Ranking
        df_ranked = df[['employee_id', 'directorate', 'final_match_rate', 'is_benchmark']].drop_duplicates()
        
        # 2. Merge info detail & CLEAN UP NULLS
        df_ranked = pd.merge(df_ranked, employee_list_df[['employee_id', 'fullname', 'role', 'grade']], on='employee_id', how='left')
        
        # !!! FIX BUG: Drop rows where fullname is NaN (Ghost Data) !!!
        df_ranked = df_ranked.dropna(subset=['fullname'])

        # 3. Determine Top TGV 
        df['tgv_match_rate'] = df['tgv_match_rate'].astype(float)
        df_tgv_scores = df[['employee_id', 'tgv_name', 'tgv_match_rate']].drop_duplicates()
        df_top_tgv = df_tgv_scores.sort_values('tgv_match_rate', ascending=False).groupby('employee_id').head(1)
        df_top_tgv = df_top_tgv.rename(columns={'tgv_name': 'top_tgv'})[['employee_id', 'top_tgv']]
        
        df_ranked = pd.merge(df_ranked, df_top_tgv, on='employee_id', how='left')
        df_ranked['top_tgv'] = df_ranked['top_tgv'].fillna('N/A')

        # 4. Filter Candidates Only
        df_candidates = df_ranked[df_ranked['is_benchmark'] == False].copy()
        df_candidates = df_candidates.sort_values('final_match_rate', ascending=False).reset_index(drop=True)
        df_candidates.insert(0, 'Rank', range(1, len(df_candidates) + 1))

        # 5. Display Table
        st.dataframe(
            df_candidates[['Rank', 'fullname', 'role', 'grade', 'directorate', 'top_tgv', 'final_match_rate']],
            column_config={
                "final_match_rate": st.column_config.ProgressColumn(
                    "Match Rate",
                    format="%.2f%%", 
                    min_value=0,
                    max_value=100
                ),
                "top_tgv": st.column_config.TextColumn("Top Strength")
            },
            hide_index=True, 
            use_container_width=True
        )

        # --- C. VISUALIZATIONS ---
        st.write("---") 
        st.subheader("📈 Dashboard Visualizations")
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Final Match Rate Distribution**")
            if not df_candidates.empty:
                fig_hist = px.histogram(df_candidates, x="final_match_rate", nbins=20,
                                        color_discrete_sequence=['#636EFA'])
                fig_hist.update_layout(bargap=0.1, xaxis_title="Match Rate (%)", yaxis_title="Count")
                st.plotly_chart(fig_hist, use_container_width=True)
            else:
                st.warning("No candidate data available.")

        with col2:
            st.markdown("**Average Match by Category**")
            df_tgv_cand = df[df['is_benchmark'] == False][['tgv_name', 'tgv_match_rate']].drop_duplicates()
            avg_tgv = df_tgv_cand.groupby('tgv_name')['tgv_match_rate'].mean().reset_index().sort_values('tgv_match_rate')
            
            fig_bar = px.bar(avg_tgv, x='tgv_match_rate', y='tgv_name', orientation='h',
                             text_auto='.1f', color='tgv_match_rate', color_continuous_scale='Blues')
            
            # !!! FIX BUG: Hide Legend Color Bar !!!
            fig_bar.update_layout(
                xaxis_range=[0,100], 
                xaxis_title="Avg Match (%)", 
                yaxis_title="",
                coloraxis_showscale=False 
            )
            st.plotly_chart(fig_bar, use_container_width=True)

        # --- D. BENCHMARK VS CANDIDATE (RADAR) ---
        st.write("---") 
        st.subheader("🔍 Benchmark vs. Candidate Comparison")

        if not df_candidates.empty:
            cand_opts = df_candidates['fullname'] + " (" + df_candidates['employee_id'] + ")"
            selected_cand_str = st.selectbox("Select Candidate:", options=cand_opts)
            
            if selected_cand_str:
                selected_cand_id = selected_cand_str.split('(')[-1].replace(')', '')

                # Prepare Data
                df_bench = df[df['is_benchmark'] == True][['tgv_name', 'tgv_match_rate']].drop_duplicates()
                bench_avg = df_bench.groupby('tgv_name')['tgv_match_rate'].mean()

                df_cand = df[df['employee_id'] == selected_cand_id][['tgv_name', 'tgv_match_rate']].drop_duplicates()
                cand_score = df_cand.set_index('tgv_name')['tgv_match_rate']

                categories = ['Competency', 'Psychometric (Cognitive)', 'Psychometric (Personality)', 
                              'Behavioral (Strengths)', 'Contextual (Background)']
                
                # !!! FIX BUG: Convert DECIMAL to FLOAT explicitly !!!
                r_bench = bench_avg.reindex(categories).fillna(0).astype(float).values
                r_cand = cand_score.reindex(categories).fillna(0).astype(float).values

                # Radar Chart
                fig_radar = go.Figure()
                fig_radar.add_trace(go.Scatterpolar(
                    r=r_bench, theta=categories, fill='toself', name='Benchmark Avg',
                    line_color='gray', opacity=0.5
                ))
                fig_radar.add_trace(go.Scatterpolar(
                    r=r_cand, theta=categories, fill='toself', name='Candidate',
                    line_color='#636EFA'
                ))
                fig_radar.update_layout(
                    polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
                    showlegend=True,
                    title=f"Comparison: {selected_cand_id} vs Benchmark"
                )
                st.plotly_chart(fig_radar, use_container_width=True)

                # Insights
                diffs = pd.Series(r_cand - r_bench, index=categories)
                st.markdown("**Quick Insights:**")
                col_in1, col_in2 = st.columns(2)
                with col_in1:
                    strongest = diffs.idxmax()
                    st.success(f"💪 **Strongest vs Bench:** {strongest} (+{diffs.max():.1f}%)")
                with col_in2:
                    weakest = diffs.idxmin()
                    st.error(f"⚠️ **Gap vs Bench:** {weakest} ({diffs.min():.1f}%)")

    except Exception as e:
        st.error(f"Error displaying results: {e}") 