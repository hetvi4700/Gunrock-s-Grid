import pandas as pd
import numpy as np
import plotly.express as px
import streamlit as st

# =========================================================
# PAGE CONFIG
# =========================================================
st.set_page_config(
    page_title="Compare Listings",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("Compare Housing Listings")
st.caption("Compare up to 3 selected listings side-by-side")

# =========================================================
# DATA LOADING
# =========================================================
DATA_PATH = "/Users/hetvi/Documents/Stats/Gunrock-s-Grid/enriched_listings.csv"

@st.cache_data
def load_data(path):
    return pd.read_csv(path)

df = load_data(DATA_PATH)

# =========================================================
# BASIC CLEANING
# =========================================================
df = df.dropna(
    subset=["listing_id", "lat", "lon", "complex_name", "address", "price_total", "bedrooms", "price_per_bed"]
).copy()

numeric_cols = [
    "price_total", "bedrooms", "baths", "sqft", "lat", "lon",
    "price_per_bed", "price_per_sqft",
    "dist_to_memorial_union_mu", "dist_to_silo", "dist_to_shields_library",
    "dist_to_arc_activities_and_recreation_center", "dist_to_student_health_center",
    "dist_to_trader_joes", "dist_to_safeway_north",
    "dist_to_nugget_markets_east_covell", "dist_to_davis_food_co-op",
    "dist_to_target", "dist_to_downtown_davis_3rd_and_g_st",
    "dist_to_davis_farmers_market", "dist_to_davis_amtrak_station",
    "nearest_grocery_dist", "nearest_campus_dist"
]

for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

df = df[df["price_per_bed"] > 0].copy()

for col in ["pets_allowed", "has_parking"]:
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip().str.lower()

# =========================================================
# HELPERS
# =========================================================
def minmax_score(series, higher_is_better=False):
    s = pd.to_numeric(series, errors="coerce")
    if s.notna().sum() == 0:
        return pd.Series(np.zeros(len(s)), index=s.index)

    s_min = s.min()
    s_max = s.max()

    if pd.isna(s_min) or pd.isna(s_max) or s_max == s_min:
        return pd.Series(np.full(len(s), 50.0), index=s.index)

    norm = (s - s_min) / (s_max - s_min)
    return norm * 100 if higher_is_better else (1 - norm) * 100


def yes_no_pretty(val):
    if pd.isna(val):
        return "N/A"
    s = str(val).strip().lower()
    if any(x in s for x in ["yes", "true", "allowed"]):
        return "Yes"
    if any(x in s for x in ["no", "false"]):
        return "No"
    return str(val)


def build_metric_row(df_in, label, col_name, fmt="text", higher_better=None):
    if col_name not in df_in.columns:
        return None

    values = df_in[col_name]

    if fmt == "currency":
        formatted = values.apply(lambda x: f"${x:,.0f}" if pd.notna(x) else "N/A")
    elif fmt == "float1":
        formatted = values.apply(lambda x: f"{x:.1f}" if pd.notna(x) else "N/A")
    elif fmt == "float2":
        formatted = values.apply(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
    elif fmt == "int":
        formatted = values.apply(lambda x: f"{int(round(x))}" if pd.notna(x) else "N/A")
    elif fmt == "bool":
        formatted = values.apply(yes_no_pretty)
    else:
        formatted = values.fillna("N/A").astype(str)

    row = pd.DataFrame([formatted.values], columns=df_in["address"].tolist(), index=[label])

    if higher_better is not None:
        valid = pd.to_numeric(values, errors="coerce")
        if valid.notna().sum() > 0:
            best_val = valid.max() if higher_better else valid.min()
            best_mask = valid == best_val
            for i, is_best in enumerate(best_mask):
                if is_best:
                    col = df_in["address"].iloc[i]
                    row.loc[label, col] = f"⭐ {row.loc[label, col]}"

    return row


def style_comparison_table(comp_table):
    def highlight_star(val):
        if isinstance(val, str) and val.startswith("⭐"):
            return "background-color: #dff6dd; font-weight: 600;"
        return ""
    return comp_table.style.map(highlight_star)

# =========================================================
# SCORE CALCULATION
# =========================================================
df["rent_score"] = minmax_score(df["price_per_bed"], higher_is_better=False)

if "nearest_campus_dist" in df.columns:
    df["campus_score"] = minmax_score(df["nearest_campus_dist"], higher_is_better=False)
else:
    df["campus_score"] = 50.0

if "nearest_grocery_dist" in df.columns:
    df["grocery_score"] = minmax_score(df["nearest_grocery_dist"], higher_is_better=False)
else:
    df["grocery_score"] = 50.0

if "dist_to_downtown_davis_3rd_and_g_st" in df.columns:
    df["social_score"] = minmax_score(df["dist_to_downtown_davis_3rd_and_g_st"], higher_is_better=False)
else:
    df["social_score"] = 50.0

df["student_score"] = (
    0.45 * df["rent_score"] +
    0.30 * df["campus_score"] +
    0.15 * df["grocery_score"] +
    0.10 * df["social_score"]
)

# =========================================================
# LOAD SELECTED IDS
# =========================================================
selected_listing_ids = st.session_state.get("selected_listing_ids", [])

if not selected_listing_ids:
    st.warning("No listings were selected on the main page. Go back and select up to 3 rows from the recommendations table.")
    st.stop()

comparison_data = df[df["listing_id"].isin(selected_listing_ids)].copy()

if comparison_data.empty:
    st.warning("The selected listing IDs were not found in the dataset.")
    st.stop()

comparison_data["listing_id"] = comparison_data["listing_id"].astype(str)
selected_listing_ids = [str(x) for x in selected_listing_ids]

comparison_data = comparison_data.set_index("listing_id").loc[selected_listing_ids].reset_index()

st.success(f"Loaded {len(comparison_data)} listing(s) from the main page.")

# =========================================================
# QUICK WINNERS
# =========================================================
best_score_idx = comparison_data["student_score"].idxmax()
cheapest_idx = comparison_data["price_per_bed"].idxmin()

metric_cols = st.columns(3)
metric_cols[0].metric("Best Overall", comparison_data.loc[best_score_idx, "address"])
metric_cols[1].metric("Lowest Price / Bed", comparison_data.loc[cheapest_idx, "address"])

if "nearest_campus_dist" in comparison_data.columns and comparison_data["nearest_campus_dist"].notna().sum() > 0:
    closest_campus_idx = comparison_data["nearest_campus_dist"].idxmin()
    metric_cols[2].metric("Closest to Campus", comparison_data.loc[closest_campus_idx, "address"])

# =========================================================
# TABS
# =========================================================
tab1, tab2, tab3 = st.tabs(["Comparison Table", "Score Breakdown", "Visual Comparison"])

# =========================================================
# TAB 1: COMPARISON TABLE
# =========================================================
with tab1:
    st.write("### Detailed side-by-side comparison")

    comparison_rows = []
    row_specs = [
        ("Listing ID", "listing_id", "text", None),
        ("Address", "address", "text", None),
        ("Complex Name", "complex_name", "text", None),
        ("Neighborhood", "neighborhood", "text", None),
        ("Price Total", "price_total", "currency", False),
        ("Price per Bed", "price_per_bed", "currency", False),
        ("Bedrooms", "bedrooms", "int", True),
        ("Bathrooms", "baths", "float1", True),
        ("Square Feet", "sqft", "int", True),
        ("Price per Sq Ft", "price_per_sqft", "float2", False),
        ("Nearest Campus", "nearest_campus", "text", None),
        ("Distance to Campus", "nearest_campus_dist", "float2", False),
        ("Nearest Grocery", "nearest_grocery", "text", None),
        ("Distance to Grocery", "nearest_grocery_dist", "float2", False),
        ("Distance to Downtown", "dist_to_downtown_davis_3rd_and_g_st", "float2", False),
        ("Pets Allowed", "pets_allowed", "bool", None),
        ("Parking", "has_parking", "bool", None),
        ("Laundry Type", "laundry_type", "text", None),
        ("Student Score", "student_score", "float1", True),
    ]

    for label, col_name, fmt, higher_better in row_specs:
        row_df = build_metric_row(comparison_data, label, col_name, fmt=fmt, higher_better=higher_better)
        if row_df is not None:
            comparison_rows.append(row_df)

    if comparison_rows:
        comparison_table = pd.concat(comparison_rows, axis=0)
        st.dataframe(style_comparison_table(comparison_table), use_container_width=True)
    else:
        st.warning("No comparable columns found for the selected listings.")

# =========================================================
# TAB 2: SCORE BREAKDOWN
# =========================================================
with tab2:
    st.write("### Score breakdown")

    score_data = comparison_data[["address", "rent_score", "campus_score", "grocery_score", "social_score", "student_score"]].copy()
    score_long = score_data.melt(
        id_vars="address",
        var_name="metric",
        value_name="score"
    )

    score_long["metric"] = score_long["metric"].map({
        "rent_score": "Rent",
        "campus_score": "Campus",
        "grocery_score": "Grocery",
        "social_score": "Social",
        "student_score": "Overall"
    })

    fig_scores = px.bar(
        score_long,
        x="metric",
        y="score",
        color="address",
        barmode="group",
        title="Score Breakdown by Listing",
        color_discrete_sequence=px.colors.qualitative.Set1
    )
    fig_scores.update_layout(
        template="plotly_white",
        xaxis_title="",
        yaxis_title="Score",
        legend_title_text="Address"
    )
    st.plotly_chart(fig_scores, use_container_width=True)

    raw_cols = [
        c for c in [
            "address",
            "price_per_bed",
            "nearest_campus_dist",
            "nearest_grocery_dist",
            "dist_to_downtown_davis_3rd_and_g_st"
        ] if c in comparison_data.columns
    ]

    if raw_cols:
        st.write("### Raw score inputs")
        st.dataframe(comparison_data[raw_cols], use_container_width=True)

# =========================================================
# TAB 3: VISUAL COMPARISON
# =========================================================
with tab3:
    st.write("### Meaningful visual comparison")

    if "nearest_campus_dist" in comparison_data.columns:
        fig_scatter = px.scatter(
            comparison_data,
            x="price_per_bed",
            y="nearest_campus_dist",
            size="student_score",
            text="address",
            hover_name="address",
            title="Affordability vs Campus Distance"
        )
        fig_scatter.update_traces(textposition="top center")
        fig_scatter.update_layout(
            template="plotly_white",
            xaxis_title="Price per Bed ($)",
            yaxis_title="Distance to Campus (miles)"
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

    col1, col2 = st.columns(2)

    with col1:
        fig_price = px.bar(
            comparison_data,
            x="address",
            y="price_per_bed",
            text="price_per_bed",
            title="Price per Bed",
            color="address",
            color_discrete_sequence=px.colors.qualitative.Set1
        )
        fig_price.update_traces(texttemplate="$%{y:.0f}", textposition="outside")
        fig_price.update_layout(
            template="plotly_white",
            xaxis_title="",
            yaxis_title="Price per Bed ($)",
            showlegend=False
        )
        st.plotly_chart(fig_price, use_container_width=True)

    with col2:
        fig_score = px.bar(
            comparison_data,
            x="address",
            y="student_score",
            text="student_score",
            title="Overall Student Score",
            color="address",
            color_discrete_sequence=px.colors.qualitative.Set1
        )
        fig_score.update_traces(texttemplate="%{y:.1f}", textposition="outside")
        fig_score.update_layout(
            template="plotly_white",
            xaxis_title="",
            yaxis_title="Student Score",
            showlegend=False
        )
        st.plotly_chart(fig_score, use_container_width=True)

    dist_metrics = [
        c for c in [
            "nearest_campus_dist",
            "nearest_grocery_dist",
            "dist_to_downtown_davis_3rd_and_g_st"
        ] if c in comparison_data.columns
    ]

    if dist_metrics:
        dist_long = comparison_data[["address"] + dist_metrics].melt(
            id_vars="address",
            var_name="place",
            value_name="distance"
        )

        dist_long["place"] = dist_long["place"].map({
            "nearest_campus_dist": "Campus",
            "nearest_grocery_dist": "Grocery",
            "dist_to_downtown_davis_3rd_and_g_st": "Downtown"
        })

        fig_dist = px.bar(
            dist_long,
            x="place",
            y="distance",
            color="address",
            barmode="group",
            title="Distance Comparison",
            color_discrete_sequence=px.colors.qualitative.Set1
        )
        fig_dist.update_layout(
            template="plotly_white",
            xaxis_title="",
            yaxis_title="Distance (miles)",
            legend_title_text="Address"
        )
        st.plotly_chart(fig_dist, use_container_width=True)

    value_metrics = [
        c for c in ["sqft", "bedrooms", "baths"] if c in comparison_data.columns
    ]

    if value_metrics:
        st.write("_Space and layout metrics are available in the comparison table above._")