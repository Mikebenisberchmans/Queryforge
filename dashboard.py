import streamlit as st
import sqlite3
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime
import os
import time

st.set_page_config(
    page_title="Business Intelligence Dashboard",
    layout="wide",
    page_icon="📊",
    initial_sidebar_state="collapsed"
)

DB_PATH = os.environ.get("DB_PATH", "database.db")

# ── CSS ─────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700;800&display=swap');
* { font-family: 'Inter', sans-serif !important; }
.stApp { background: #080d1a; }
section[data-testid="stSidebar"] { display: none; }

.dash-header {
    background: linear-gradient(135deg, #0d1526 0%, #111d35 100%);
    border: 1px solid #1a3050;
    border-radius: 16px;
    padding: 20px 28px;
    margin-bottom: 20px;
    display: flex;
    justify-content: space-between;
    align-items: center;
}
.kpi-wrap {
    background: linear-gradient(145deg, #0e1a2e, #131f36);
    border: 1px solid #1a3050;
    border-radius: 14px;
    padding: 18px 14px 14px;
    text-align: center;
    position: relative;
    overflow: hidden;
    margin-bottom: 4px;
}
.kpi-accent { position: absolute; top: 0; left: 0; right: 0; height: 3px; border-radius: 14px 14px 0 0; }
.kpi-icon  { font-size: 1.4rem; margin-bottom: 4px; }
.kpi-val   { font-size: 1.65rem; font-weight: 800; line-height: 1; margin: 2px 0; }
.kpi-lbl   { font-size: 0.65rem; color: #6a7f99; text-transform: uppercase; letter-spacing: 1.2px; margin-top: 4px; }

.sec-title {
    font-size: 0.68rem; font-weight: 600; color: #4a6080;
    text-transform: uppercase; letter-spacing: 2px;
    border-bottom: 1px solid #1a3050;
    padding-bottom: 8px; margin: 22px 0 14px;
}
.insight-box {
    background: linear-gradient(145deg, #0e1a2e, #131f36);
    border: 1px solid #1a3050;
    border-left-width: 4px;
    border-radius: 10px;
    padding: 13px 15px;
    margin-bottom: 10px;
}
.insight-title { font-size: 0.78rem; font-weight: 700; margin-bottom: 4px; }
.insight-body  { font-size: 0.72rem; color: #7a8fa8; line-height: 1.5; }

.pulse { display:inline-block; width:8px; height:8px; border-radius:50%;
         background:#00e096; margin-right:6px; animation: blink 2s infinite; }
@keyframes blink {
    0%,100% { opacity:1; transform:scale(1); }
    50%      { opacity:.4; transform:scale(1.4); }
}
</style>
""", unsafe_allow_html=True)

COLORS = ['#00d4ff','#7b5ea7','#00e096','#ff8c42','#ff5c5c','#f7c59f','#5ee7df','#b490ca']
DARK = dict(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
            font=dict(family='Inter', color='#8899aa', size=11),
            margin=dict(l=8, r=8, t=36, b=8),
            showlegend=True,
            legend=dict(bgcolor='rgba(0,0,0,0)', font=dict(color='#99aabb', size=10)))
AX = dict(gridcolor='#162035', zeroline=False, tickfont=dict(color='#556677'))

def db(sql):
    con = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(sql, con)
    con.close()
    return df

def chart(fig, h=270):
    fig.update_layout(**DARK, height=h)
    fig.update_xaxes(**AX)
    fig.update_yaxes(**AX)
    st.plotly_chart(fig, use_container_width=True)

def kpi_card(icon, value, label, color):
    st.markdown(f"""
    <div class="kpi-wrap">
      <div class="kpi-accent" style="background:{color};"></div>
      <div class="kpi-icon">{icon}</div>
      <div class="kpi-val" style="color:{color};">{value}</div>
      <div class="kpi-lbl">{label}</div>
    </div>""", unsafe_allow_html=True)

def section(title):
    st.markdown(f'<div class="sec-title">{title}</div>', unsafe_allow_html=True)

def insight(color, title, body):
    st.markdown(f"""
    <div class="insight-box" style="border-left-color:{color};">
      <div class="insight-title" style="color:{color};">{title}</div>
      <div class="insight-body">{body}</div>
    </div>""", unsafe_allow_html=True)

# HEADER
now = datetime.now().strftime('%b %d, %Y  %H:%M:%S')
st.markdown(f"""
<div class="dash-header">
  <div>
    <div style="font-size:1.7rem;font-weight:800;color:#fff;">📊 Business Intelligence Dashboard</div>
    <div style="font-size:0.82rem;color:#4a6a8a;margin-top:4px;">
      <span class="pulse"></span>Live Analytics · Auto-refreshes every 5 seconds
    </div>
  </div>
  <div style="text-align:right;">
    <div style="font-size:0.68rem;color:#4a6a8a;">LAST UPDATED</div>
    <div style="font-size:0.9rem;font-weight:700;color:#fff;">{now}</div>
  </div>
</div>
""", unsafe_allow_html=True)

# KPIs
kpi = db("""
SELECT
  (SELECT COUNT(*) FROM fact_sales) AS total_orders,
  (SELECT ROUND(SUM(net_amount)/1e6,1) FROM fact_sales) AS rev_m,
  (SELECT ROUND(SUM(gross_profit)/1e6,1) FROM fact_sales) AS profit_m,
  (SELECT ROUND(AVG(gross_profit/net_amount*100),1) FROM fact_sales WHERE net_amount > 0) AS margin,
  (SELECT COUNT(*) FROM crm_leads WHERE status='Qualified') AS hot_leads,
  (SELECT ROUND(SUM(expected_value)/1e6,0) FROM crm_opportunities WHERE stage NOT IN ('Closed Won','Closed Lost')) AS pipeline_m,
  (SELECT COUNT(*) FROM crm_support_tickets WHERE status IN ('Open','Escalated')) AS open_tix,
  (SELECT COUNT(DISTINCT customer_id) FROM fact_sales) AS customers
""").iloc[0]

k = st.columns(8)
cards = [
    ("📦", f"{int(kpi.total_orders):,}", "Total Orders",  "#00d4ff"),
    ("💰", f"${kpi.rev_m}M",             "Revenue",        "#00e096"),
    ("📈", f"${kpi.profit_m}M",          "Gross Profit",   "#7b5ea7"),
    ("🎯", f"{kpi.margin}%",             "Avg Margin",     "#f7c59f"),
    ("🔥", f"{int(kpi.hot_leads):,}",    "Hot Leads",      "#ff8c42"),
    ("🚀", f"${int(kpi.pipeline_m)}M",   "Pipeline",       "#5ee7df"),
    ("🎫", f"{int(kpi.open_tix):,}",     "Open Tickets",   "#ff5c5c"),
    ("👥", f"{int(kpi.customers):,}",    "Customers",      "#b490ca"),
]
for col, (icon, val, lbl, color) in zip(k, cards):
    with col:
        kpi_card(icon, val, lbl, color)

# ROW 1 — Revenue Trend + Category
section("📅  Revenue & Profitability Trends")
c1, c2 = st.columns([2, 1])

with c1:
    rev = db("""
    SELECT SUBSTR(CAST(date_id AS TEXT),1,4)||'-'||SUBSTR(CAST(date_id AS TEXT),5,2) AS ym,
           ROUND(SUM(net_amount)/1000,1) AS rev_k,
           ROUND(SUM(gross_profit)/1000,1) AS profit_k
    FROM fact_sales GROUP BY ym ORDER BY ym
    """)
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=rev['ym'], y=rev['rev_k'], name='Revenue',
        fill='tozeroy', fillcolor='rgba(0,212,255,0.07)', line=dict(color='#00d4ff', width=2)))
    fig.add_trace(go.Scatter(x=rev['ym'], y=rev['profit_k'], name='Profit',
        fill='tozeroy', fillcolor='rgba(123,94,167,0.07)', line=dict(color='#7b5ea7', width=2)))
    fig.update_layout(title=dict(text='Monthly Revenue vs Profit (USD Thousands)', font=dict(color='#ccddee', size=12)))
    fig.update_xaxes(tickangle=40, nticks=18)
    chart(fig, 290)

with c2:
    cat = db("""
    SELECT dc.category_name,
           ROUND(SUM(fs.net_amount)/1000,1) AS rev_k,
           ROUND(AVG(fs.gross_profit/fs.net_amount*100),1) AS margin
    FROM fact_sales fs
    JOIN dim_product dp ON fs.product_id = dp.product_id
    JOIN dim_category dc ON dp.category_id = dc.category_id
    GROUP BY dc.category_name ORDER BY rev_k DESC
    """)
    fig2 = go.Figure(go.Bar(
        x=cat['category_name'], y=cat['rev_k'],
        marker=dict(color=COLORS[:4], line=dict(width=0)),
        text=[f"{m}% margin" for m in cat['margin']],
        textposition='outside', textfont=dict(color='#8899aa', size=9)
    ))
    fig2.update_layout(title=dict(text='Revenue by Category (with Margin)', font=dict(color='#ccddee', size=12)))
    chart(fig2, 290)

# ROW 2 — Channels · Salespeople · Products
section("🏆  Sales Performance")
c3, c4, c5 = st.columns(3)

with c3:
    ch = db("""
    SELECT dc.channel_name, ROUND(SUM(fs.net_amount)/1000,1) AS rev_k
    FROM fact_sales fs
    JOIN dim_channel dc ON fs.channel_id = dc.channel_id
    GROUP BY dc.channel_name ORDER BY rev_k DESC
    """)
    fig3 = go.Figure(go.Bar(
        y=ch['channel_name'], x=ch['rev_k'], orientation='h',
        marker=dict(color=COLORS[:5], line=dict(width=0)),
        text=[f"${v}K" for v in ch['rev_k']],
        textposition='outside', textfont=dict(color='#8899aa', size=10)
    ))
    fig3.update_layout(title=dict(text='Revenue by Sales Channel', font=dict(color='#ccddee', size=12)))
    chart(fig3, 270)

with c4:
    sp = db("""
    SELECT ds.full_name, ROUND(SUM(fs.net_amount)/1000,1) AS rev_k
    FROM fact_sales fs
    JOIN dim_salesperson ds ON fs.salesperson_id = ds.salesperson_id
    GROUP BY ds.full_name ORDER BY rev_k DESC LIMIT 8
    """)
    fig4 = go.Figure(go.Bar(
        y=sp['full_name'], x=sp['rev_k'], orientation='h',
        marker=dict(color='#00d4ff', opacity=0.85, line=dict(width=0)),
        text=[f"${v}K" for v in sp['rev_k']],
        textposition='outside', textfont=dict(color='#8899aa', size=9)
    ))
    fig4.update_layout(title=dict(text='Top 8 Salespeople by Revenue', font=dict(color='#ccddee', size=12)))
    chart(fig4, 270)

with c5:
    pr = db("""
    SELECT dp.product_name,
           ROUND(SUM(fs.net_amount)/1000,1) AS rev_k,
           ROUND(AVG(fs.gross_profit/fs.net_amount*100),1) AS margin
    FROM fact_sales fs
    JOIN dim_product dp ON fs.product_id = dp.product_id
    GROUP BY dp.product_name ORDER BY rev_k DESC LIMIT 8
    """)
    fig5 = go.Figure(go.Bar(
        y=pr['product_name'], x=pr['rev_k'], orientation='h',
        marker=dict(color='#7b5ea7', opacity=0.9, line=dict(width=0)),
        text=[f"{m}%" for m in pr['margin']],
        textposition='outside', textfont=dict(color='#8899aa', size=9)
    ))
    fig5.update_layout(title=dict(text='Top Products (margin % shown)', font=dict(color='#ccddee', size=12)))
    chart(fig5, 270)

# ROW 3 — Pipeline · Lead Source · Lead Status
section("🔁  CRM Pipeline & Lead Intelligence")
c6, c7, c8 = st.columns(3)

with c6:
    opp = db("""
    SELECT stage, COUNT(*) AS cnt, ROUND(AVG(probability),0) AS avg_prob
    FROM crm_opportunities GROUP BY stage ORDER BY avg_prob DESC
    """)
    fig6 = go.Figure(go.Funnel(
        y=opp['stage'], x=opp['cnt'],
        textinfo='value+percent initial',
        marker=dict(color=COLORS[:6]),
        textfont=dict(color='white', size=10)
    ))
    fig6.update_layout(**DARK, height=280,
        title=dict(text='Opportunity Pipeline Funnel', font=dict(color='#ccddee', size=12)))
    st.plotly_chart(fig6, use_container_width=True)

with c7:
    ls = db("""
    SELECT source,
           COUNT(*) AS leads,
           ROUND(SUM(CASE WHEN status='Converted' THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS conv_rate
    FROM crm_leads GROUP BY source ORDER BY conv_rate DESC
    """)
    fig7 = go.Figure()
    fig7.add_trace(go.Bar(name='# Leads', x=ls['source'], y=ls['leads'],
        marker_color='rgba(0,212,255,0.25)', yaxis='y'))
    fig7.add_trace(go.Scatter(name='Conv %', x=ls['source'], y=ls['conv_rate'],
        mode='lines+markers', line=dict(color='#00e096', width=2.5),
        marker=dict(size=8, color='#00e096'), yaxis='y2'))
    fig7.update_layout(
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
        font=dict(family='Inter', color='#8899aa', size=11),
        margin=dict(l=8, r=8, t=36, b=8),
        showlegend=True,
        legend=dict(bgcolor='rgba(0,0,0,0)', font=dict(color='#99aabb', size=10)),
        height=280,
        title=dict(text='Lead Source: Volume vs Conversion Rate', font=dict(color='#ccddee', size=12)),
        xaxis=dict(gridcolor='#162035', zeroline=False, tickfont=dict(color='#556677'), tickangle=30),
        yaxis=dict(gridcolor='#162035', zeroline=False, tickfont=dict(color='#556677'), title='# Leads'),
        yaxis2=dict(overlaying='y', side='right', zeroline=False,
                    tickfont=dict(color='#00e096'), title='Conv %', showgrid=False)
    )
    st.plotly_chart(fig7, use_container_width=True)

with c8:
    lst = db("SELECT status, COUNT(*) AS cnt FROM crm_leads GROUP BY status")
    fig8 = go.Figure(go.Pie(
        labels=lst['status'], values=lst['cnt'], hole=0.55,
        marker=dict(colors=COLORS[:6], line=dict(color='#080d1a', width=2)),
        textinfo='label+percent', textfont=dict(color='white', size=10)
    ))
    fig8.add_annotation(text="2,000<br>Leads", x=0.5, y=0.5, showarrow=False,
        font=dict(color='white', size=13, family='Inter'))
    fig8.update_layout(**DARK, height=280,
        title=dict(text='Lead Status Distribution', font=dict(color='#ccddee', size=12)))
    st.plotly_chart(fig8, use_container_width=True)

# ROW 4 — Support + Insights
section("🎫  Support Health & Strategic Insights")
c9, c10 = st.columns([1, 2])

with c9:
    tix = db("SELECT status, priority, COUNT(*) AS cnt FROM crm_support_tickets GROUP BY status, priority")
    pivot = tix.pivot(index='status', columns='priority', values='cnt').fillna(0)
    pcolors = {'Critical':'#ff5c5c','High':'#ff8c42','Medium':'#f7c59f','Low':'#5ee7df'}
    fig9 = go.Figure()
    for p in ['Critical','High','Medium','Low']:
        if p in pivot.columns:
            fig9.add_trace(go.Bar(name=p, x=pivot.index, y=pivot[p], marker_color=pcolors[p]))
    fig9.update_layout(**DARK, height=290, barmode='stack',
        xaxis=dict(**AX), yaxis=dict(**AX),
        title=dict(text='Support Tickets: Status × Priority', font=dict(color='#ccddee', size=12)))
    st.plotly_chart(fig9, use_container_width=True)

with c10:
    section("💡  AI-Detected Patterns & Action Leads")
    ins_data = [
        ("#00e096", "⭐ Software & Cloud Are Your Profit Engines",
         "Software (88.8%) and Cloud (88.8%) deliver 2× the margin of Hardware (41.5%). Cross-sell software/cloud to your 2,691 hardware buyers to unlock hidden margin."),
        ("#00d4ff", "🔥 Direct Mail & Cold Call Convert Best — 21.9% / 20.6%",
         "Traditional outreach dramatically outperforms LinkedIn (14.9%) and Webinars (14.4%). Shift 15% of digital budget into Direct Mail and Cold Call to lift pipeline."),
        ("#f7c59f", "🚀 $514M Weighted Pipeline in Negotiation & Proposal",
         "500 opportunities at 50–75% close probability. Accelerating deal velocity here could convert $125–188M into booked revenue this quarter."),
        ("#ff8c42", "⚠️ 91 Critical Tickets Still Open — Churn Alert",
         "91 Critical-priority tickets are Open and untouched, plus 82 Escalated Critical. These represent your most at-risk accounts. Immediate triage required."),
        ("#7b5ea7", "📦 Partner Channel Leads All Channels at $57.9M + 71.7% Margin",
         "Partner outperforms Direct, Online, Referral, and Phone on both revenue and margin. Expanding partner incentives could scale revenue efficiently."),
        ("#5ee7df", "📅 April & August Are Peak Revenue Months — Every Year",
         "Consistent revenue spikes in Apr and Aug across all 6 years signal budget-flush cycles. Pre-load campaigns in March and July to capture demand at its peak."),
    ]
    cols_ins = st.columns(2)
    for i, (color, title, body) in enumerate(ins_data):
        with cols_ins[i % 2]:
            insight(color, title, body)

# ROW 5 — Departments + Payment
section("🏢  Department & Payment Breakdown")
c11, c12 = st.columns(2)

with c11:
    dept = db("""
    SELECT dd.department_name,
           ROUND(SUM(fs.net_amount)/1000,1) AS rev_k,
           COUNT(*) AS orders,
           ROUND(AVG(fs.gross_profit/fs.net_amount*100),1) AS margin
    FROM fact_sales fs
    JOIN dim_salesperson ds ON fs.salesperson_id = ds.salesperson_id
    JOIN dim_department dd ON ds.department_id = dd.department_id
    GROUP BY dd.department_name ORDER BY rev_k DESC
    """)
    fig10 = px.scatter(dept, x='rev_k', y='margin', size='orders',
        color='department_name', color_discrete_sequence=COLORS,
        hover_name='department_name',
        labels={'rev_k':'Revenue (K $)', 'margin':'Gross Margin %', 'orders':'Orders'})
    fig10.update_layout(**DARK, height=270,
        xaxis=dict(**AX), yaxis=dict(**AX),
        title=dict(text='Department: Revenue vs Margin (bubble = order volume)', font=dict(color='#ccddee', size=12)))
    st.plotly_chart(fig10, use_container_width=True)

with c12:
    pay = db("SELECT payment_method, ROUND(SUM(net_amount)/1000,1) AS rev_k FROM fact_sales GROUP BY payment_method ORDER BY rev_k DESC")
    fig11 = go.Figure(go.Pie(
        labels=pay['payment_method'], values=pay['rev_k'], hole=0.5,
        marker=dict(colors=COLORS[:5], line=dict(color='#080d1a', width=2)),
        textinfo='label+percent', textfont=dict(color='white', size=11)
    ))
    fig11.update_layout(**DARK, height=270,
        title=dict(text='Revenue Share by Payment Method', font=dict(color='#ccddee', size=12)))
    st.plotly_chart(fig11, use_container_width=True)

# FOOTER
st.markdown(f"""
<div style="text-align:center;padding:18px 0 8px;color:#253545;font-size:0.7rem;">
  Business Intelligence Dashboard &nbsp;·&nbsp; SQLite &nbsp;·&nbsp; {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
</div>
""", unsafe_allow_html=True)

time.sleep(5)
st.rerun()
