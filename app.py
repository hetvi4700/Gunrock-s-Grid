import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# =========================================================
# PAGE CONFIG
# =========================================================
st.set_page_config(
    page_title="Gunrocks-Grid-Aggie Housing Analysis",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("Gunrocks-Grid-Aggie Housing Analysis")
st.caption("Find the best UC Davis housing based on rent, campus proximity, and amenities.")

# =========================================================
# DATA LOADING
# =========================================================
DATA_PATH = "./enriched_listings.csv"

@st.cache_data
def load_data(path):
    return pd.read_csv(path)

df = load_data(DATA_PATH)

# =========================================================
# BASIC CLEANING
# =========================================================
df = df.dropna(
    subset=["listing_id", "lat", "lon", "complex_name", "price_total", "bedrooms", "price_per_bed"]
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
# POI DEFINITIONS
# =========================================================
PLACES_OF_INTEREST = [
    {
        "name": "Memorial Union (MU)",
        "lat": 38.5423,
        "lon": -121.7496,
        "category": "campus",
        "description": "main student union building, center of campus life",
        "dist_col": "dist_to_memorial_union_mu"
    },
    {
        "name": "Silo",
        "lat": 38.5382,
        "lon": -121.7527,
        "category": "campus",
        "description": "food court area between classes, south side of campus",
        "dist_col": "dist_to_silo"
    },
    {
        "name": "Shields Library",
        "lat": 38.5397,
        "lon": -121.7495,
        "category": "campus",
        "description": "main library - where everyone studies",
        "dist_col": "dist_to_shields_library"
    },
    {
        "name": "ARC (Activities & Recreation Center)",
        "lat": 38.5428,
        "lon": -121.7591,
        "category": "campus",
        "description": "gym / rec center on west side of campus",
        "dist_col": "dist_to_arc_activities_and_recreation_center"
    },
    {
        "name": "Student Health Center",
        "lat": 38.5427,
        "lon": -121.7615,
        "category": "campus",
        "description": "health and counseling services",
        "dist_col": "dist_to_student_health_center"
    },
    {
        "name": "Trader Joes",
        "lat": 38.5467,
        "lon": -121.7615,
        "category": "grocery",
        "description": "885 Russell Blvd - closest TJs to campus",
        "dist_col": "dist_to_trader_joes"
    },
    {
        "name": "Safeway (North)",
        "lat": 38.5621,
        "lon": -121.7663,
        "category": "grocery",
        "description": "1451 W Covell Blvd - north davis safeway",
        "dist_col": "dist_to_safeway_north"
    },
    {
        "name": "Nugget Markets (East Covell)",
        "lat": 38.5610,
        "lon": -121.7331,
        "category": "grocery",
        "description": "1414 E Covell Blvd - nicer grocery store, bit pricey",
        "dist_col": "dist_to_nugget_markets_east_covell"
    },
    {
        "name": "Davis Food Co-op",
        "lat": 38.5493,
        "lon": -121.7399,
        "category": "grocery",
        "description": "620 G St - organic/local grocery downtown",
        "dist_col": "dist_to_davis_food_co-op"
    },
    {
        "name": "Target",
        "lat": 38.5543,
        "lon": -121.6997,
        "category": "grocery",
        "description": "4601 2nd St - east davis, only target in town",
        "dist_col": "dist_to_target"
    },
    {
        "name": "Downtown Davis (3rd & G St)",
        "lat": 38.5448,
        "lon": -121.7389,
        "category": "social",
        "description": "heart of downtown - restaurants, bars, woodstocks pizza",
        "dist_col": "dist_to_downtown_davis_3rd_and_g_st"
    },
    {
        "name": "Davis Farmers Market",
        "lat": 38.5447,
        "lon": -121.7440,
        "category": "grocery",
        "description": "central park - sat mornings and wed afternoons",
        "dist_col": "dist_to_davis_farmers_market"
    },
    {
        "name": "Davis Amtrak Station",
        "lat": 38.5435,
        "lon": -121.7377,
        "category": "transit",
        "description": "train station downtown - capitol corridor to sac/bay",
        "dist_col": "dist_to_davis_amtrak_station"
    },
]

places_df = pd.DataFrame(PLACES_OF_INTEREST)
places_df = places_df[places_df["dist_col"].isin(df.columns)].copy()

# =========================================================
# SESSION STATE
# =========================================================
if "selected_listing_ids" not in st.session_state:
    st.session_state.selected_listing_ids = []

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

def parse_amenities_cell(x):
    if pd.isna(x):
        return []
    return [a.strip().lower() for a in str(x).split(",") if a.strip()]

def normalize_weights(weight_dict):
    total = sum(weight_dict.values())
    if total <= 0:
        n = len(weight_dict)
        return {k: 1 / n for k in weight_dict}
    return {k: v / total for k, v in weight_dict.items()}

# =========================================================
# SIDEBAR FILTERS
# =========================================================
st.sidebar.header("Basic Filters")

price_min = int(df["price_per_bed"].min())
price_max = int(df["price_per_bed"].max())
price_range = st.sidebar.slider(
    "Price per bed ($)",
    min_value=price_min,
    max_value=price_max,
    value=(price_min, price_max)
)

bedroom_values = sorted(df["bedrooms"].dropna().astype(int).unique().tolist())
selected_bedrooms = st.sidebar.multiselect(
    "Bedrooms",
    options=bedroom_values
)

if "neighborhood" in df.columns:
    neighborhoods = sorted(df["neighborhood"].dropna().astype(str).unique().tolist())
    selected_neighborhoods = st.sidebar.multiselect(
        "Neighborhood",
        options=neighborhoods
    )
else:
    selected_neighborhoods = []

if "pets_allowed" in df.columns:
    pets_filter = st.sidebar.selectbox(
        "Pets allowed",
        options=["Any", "Yes only", "No only"]
    )
else:
    pets_filter = "Any"

if "has_parking" in df.columns:
    parking_filter = st.sidebar.selectbox(
        "Parking",
        options=["Any", "Yes only", "No only"]
    )
else:
    parking_filter = "Any"

if "laundry_type" in df.columns:
    laundry_options = sorted(df["laundry_type"].dropna().astype(str).unique().tolist())
    selected_laundry = st.sidebar.multiselect(
        "Laundry type",
        options=laundry_options
    )
else:
    selected_laundry = []

# =========================================================
# DISTANCE FILTERS
# =========================================================
st.sidebar.header("Location Filters")

location_preferences = st.sidebar.multiselect(
    "Only show housing near these places",
    options=places_df["name"].tolist()
)

default_max_dists = {}
for place_name in location_preferences:
    default_max_dists[place_name] = st.sidebar.slider(
        f"Max distance to {place_name}",
        min_value=0.1,
        max_value=8.0,
        value=2.0,
        step=0.1,
        key=f"dist_{place_name}"
    )

# =========================================================
# AMENITY PREFERENCE + SCORING
# =========================================================
st.sidebar.header("Scoring Preferences")

rent_weight = st.sidebar.slider("Weight: Rent affordability", 0.0, 1.0, 0.45, 0.05)
campus_weight = st.sidebar.slider("Weight: Campus proximity", 0.0, 1.0, 0.30, 0.05)
grocery_weight = st.sidebar.slider("Weight: Grocery access", 0.0, 1.0, 0.15, 0.05)
social_weight = st.sidebar.slider("Weight: Social / downtown access", 0.0, 1.0, 0.10, 0.05)

poi_priority_names = st.sidebar.multiselect(
    "Extra places to prioritize in score",
    options=places_df["name"].tolist()
)

st.sidebar.subheader("Optional keyword amenities")
desired_amenities_text = st.sidebar.text_input(
    "Enter amenities you want (comma-separated)"
)
desired_amenities = [x.strip().lower() for x in desired_amenities_text.split(",") if x.strip()]

# =========================================================
# APPLY FILTERS
# =========================================================
filtered = df.copy()

filtered = filtered[
    (filtered["price_per_bed"] >= price_range[0]) &
    (filtered["price_per_bed"] <= price_range[1])
]

if selected_bedrooms:
    filtered = filtered[filtered["bedrooms"].astype(int).isin(selected_bedrooms)]

if selected_neighborhoods and "neighborhood" in filtered.columns:
    filtered = filtered[filtered["neighborhood"].astype(str).isin(selected_neighborhoods)]

if pets_filter != "Any" and "pets_allowed" in filtered.columns:
    if pets_filter == "Yes only":
        filtered = filtered[filtered["pets_allowed"].str.contains("yes|true|allowed", na=False)]
    elif pets_filter == "No only":
        filtered = filtered[~filtered["pets_allowed"].str.contains("yes|true|allowed", na=False)]

if parking_filter != "Any" and "has_parking" in filtered.columns:
    if parking_filter == "Yes only":
        filtered = filtered[filtered["has_parking"].str.contains("yes|true", na=False)]
    elif parking_filter == "No only":
        filtered = filtered[~filtered["has_parking"].str.contains("yes|true", na=False)]

if selected_laundry and "laundry_type" in filtered.columns:
    filtered = filtered[filtered["laundry_type"].astype(str).isin(selected_laundry)]

if location_preferences:
    for place_name in location_preferences:
        row = places_df[places_df["name"] == place_name]
        if not row.empty:
            dist_col = row.iloc[0]["dist_col"]
            max_dist = default_max_dists[place_name]
            if dist_col in filtered.columns:
                filtered = filtered[filtered[dist_col] <= max_dist]

# =========================================================
# AMENITY MATCH SCORE
# =========================================================
if "amenities" in filtered.columns:
    amenity_lists = filtered["amenities"].apply(parse_amenities_cell)
    if desired_amenities:
        filtered["amenity_match_count"] = amenity_lists.apply(
            lambda lst: sum(1 for a in desired_amenities if a in lst)
        )
        filtered["amenity_match_score"] = (
            filtered["amenity_match_count"] / max(len(desired_amenities), 1)
        ) * 100
    else:
        filtered["amenity_match_score"] = 0.0
else:
    filtered["amenity_match_score"] = 0.0

# =========================================================
# BUILD DYNAMIC SCORES
# =========================================================
if len(filtered) > 0:
    filtered["rent_score"] = minmax_score(filtered["price_per_bed"], higher_is_better=False)

    campus_candidates = [
        c for c in [
            "dist_to_memorial_union_mu",
            "dist_to_mu",
            "nearest_campus_dist"
        ] if c in filtered.columns
    ]
    if campus_candidates:
        filtered["campus_score"] = minmax_score(filtered[campus_candidates[0]], higher_is_better=False)
    else:
        filtered["campus_score"] = 50.0

    if "nearest_grocery_dist" in filtered.columns:
        filtered["grocery_score"] = minmax_score(filtered["nearest_grocery_dist"], higher_is_better=False)
    else:
        grocery_cols = [
            p["dist_col"]
            for _, p in places_df[places_df["category"] == "grocery"].iterrows()
            if p["dist_col"] in filtered.columns
        ]
        if grocery_cols:
            filtered["nearest_grocery_dynamic"] = filtered[grocery_cols].min(axis=1)
            filtered["grocery_score"] = minmax_score(filtered["nearest_grocery_dynamic"], higher_is_better=False)
        else:
            filtered["grocery_score"] = 50.0

    social_cols = [c for c in ["dist_to_downtown_davis_3rd_and_g_st", "dist_to_davis_farmers_market"] if c in filtered.columns]
    if social_cols:
        filtered["social_min_dist"] = filtered[social_cols].min(axis=1)
        filtered["social_score"] = minmax_score(filtered["social_min_dist"], higher_is_better=False)
    else:
        filtered["social_score"] = 50.0

    selected_priority_cols = []
    for place_name in poi_priority_names:
        row = places_df[places_df["name"] == place_name]
        if not row.empty:
            dist_col = row.iloc[0]["dist_col"]
            if dist_col in filtered.columns:
                selected_priority_cols.append(dist_col)

    if selected_priority_cols:
        filtered["priority_avg_dist"] = filtered[selected_priority_cols].mean(axis=1)
        filtered["priority_places_score"] = minmax_score(filtered["priority_avg_dist"], higher_is_better=False)
    else:
        filtered["priority_places_score"] = 50.0

    base_weights = normalize_weights({
        "rent_score": rent_weight,
        "campus_score": campus_weight,
        "grocery_score": grocery_weight,
        "social_score": social_weight
    })

    filtered["base_score"] = (
        filtered["rent_score"] * base_weights["rent_score"] +
        filtered["campus_score"] * base_weights["campus_score"] +
        filtered["grocery_score"] * base_weights["grocery_score"] +
        filtered["social_score"] * base_weights["social_score"]
    )

    filtered["student_score"] = (
        0.75 * filtered["base_score"] +
        0.15 * filtered["priority_places_score"] +
        0.10 * filtered["amenity_match_score"]
    )

# =========================================================
# TOP SUMMARY
# =========================================================
c1, c2, c3, c4 = st.columns(4)

c1.metric("Listings shown", len(filtered))
if len(filtered) > 0:
    c2.metric("Avg price / bed", f"${filtered['price_per_bed'].mean():.0f}")
    c3.metric("Avg student score", f"{filtered['student_score'].mean():.1f}")
    c4.metric("Best score", f"{filtered['student_score'].max():.1f}")
else:
    c2.metric("Avg price / bed", "N/A")
    c3.metric("Avg student score", "N/A")
    c4.metric("Best score", "N/A")

# =========================================================
# MAP + TABLE + ORIGINAL TWO GRAPHS
# =========================================================
if len(filtered) > 0:
    hover_data_dict = {
        "price_per_bed": ":.0f",
        "price_total": ":.0f",
        "address": False,
        "lat": False,
        "lon": False
    }

    if "nearest_campus_dist" in filtered.columns:
        hover_data_dict["nearest_campus_dist"] = ":.2f"
    if "nearest_grocery" in filtered.columns:
        hover_data_dict["nearest_grocery"] = True
    if "nearest_grocery_dist" in filtered.columns:
        hover_data_dict["nearest_grocery_dist"] = ":.2f"

    fig = px.scatter_map(
        filtered,
        lat="lat",
        lon="lon",
        color="student_score",
        size="bedrooms",
        size_max=22,
        zoom=12,
        center={"lat": 38.5449, "lon": -121.7405},
        hover_name="address",
        hover_data=hover_data_dict,
        color_continuous_scale="Plasma",
        title="Student Value Score Map"
    )

    category_symbols = {
        "campus": "star",
        "grocery": "marker",
        "social": "circle",
        "transit": "square"
    }

    for category in places_df["category"].unique():
        subset = places_df[places_df["category"] == category]

        fig.add_trace(
            go.Scattermap(
                lat=subset["lat"],
                lon=subset["lon"],
                mode="markers+text",
                text=subset["name"],
                textposition="top center",
                marker=dict(
                    size=13,
                    symbol=category_symbols.get(category, "marker"),
                    color="red"
                ),
                name=category.capitalize(),
                customdata=subset[["description"]],
                hovertemplate="<b>%{text}</b><br>%{customdata[0]}<extra></extra>"
            )
        )

    fig.update_layout(
        map_style="open-street-map",
        template="plotly_white",
        margin={"r": 50, "t": 50, "l": 0, "b": 0},
        title=dict(x=0.5, xanchor="center"),
        legend_title_text="Map Layers",
        legend=dict(x=0.5, y=-0.1, xanchor="center", yanchor="top", orientation="h")
    )

    st.plotly_chart(fig, use_container_width=True)

    # =====================================================
    # TABLE OF TOP RECOMMENDATIONS WITH DIRECT ROW SELECTION
    # =====================================================
    st.subheader("Top Recommendations")
    st.write("Select up to 3 listings directly from the table below, then click Compare Selected.")

    rec_cols = [
        c for c in [
            "listing_id", "complex_name", "address", "neighborhood", "price_total", "price_per_bed",
            "bedrooms", "baths", "sqft", "nearest_grocery", "nearest_grocery_dist",
            "nearest_campus", "nearest_campus_dist", "student_score"
        ] if c in filtered.columns
    ]

    top_recs = filtered.sort_values("student_score", ascending=False)[rec_cols].head(15).copy()
    top_recs = top_recs.reset_index(drop=True)

    event = st.dataframe(
        top_recs,
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="multi-row",
        key="top_recommendations_table"
    )

    selected_rows = []
    if event is not None and hasattr(event, "selection"):
        selected_rows = event.selection.rows

    selected_rows = selected_rows[:3]

    if selected_rows:
        st.session_state.selected_listing_ids = top_recs.iloc[selected_rows]["listing_id"].tolist()
    else:
        st.session_state.selected_listing_ids = []

    col1, col2, col3 = st.columns([1, 1, 3])

    with col1:
        if 0 < len(st.session_state.selected_listing_ids) <= 3:
            if st.button("🔍 Compare Selected", use_container_width=True):
                st.switch_page("pages/1_Compare.py")
        else:
            st.button("🔍 Compare Selected", use_container_width=True, disabled=True)

    with col2:
        if st.button("Clear Selection", use_container_width=True):
            st.session_state.selected_listing_ids = []
            st.rerun()

    if st.session_state.selected_listing_ids:
        st.info(f"✓ {len(st.session_state.selected_listing_ids)} listing(s) selected for comparison")

    # =====================================================
    # ORIGINAL TWO GRAPHS BELOW THE TABLE
    # =====================================================
    st.subheader("Quick Insights")

    left, right = st.columns(2)

    with left:
        fig_bar = px.bar(
            filtered.groupby("bedrooms", as_index=False)["price_per_bed"].mean(),
            x="bedrooms",
            y="price_per_bed",
            title="Average Price per Bed by Bedrooms"
        )
        fig_bar.update_layout(template="plotly_white", xaxis=dict(dtick=1))
        st.plotly_chart(fig_bar, use_container_width=True)

    with right:
        if "neighborhood" in filtered.columns:
            fig_nb = px.box(
                filtered,
                x="neighborhood",
                y="price_per_bed",
                title="Price per Bed by Neighborhood",
                color="neighborhood"
            )
            fig_nb.update_layout(template="plotly_white", showlegend=False)
            st.plotly_chart(fig_nb, use_container_width=True)

else:
    st.warning("No listings match your filters. Widen the distance, amenity, or budget settings.")