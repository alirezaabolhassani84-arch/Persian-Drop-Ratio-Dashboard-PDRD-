from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os, logging, glob, smtplib, traceback
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# =====================================================================
# تنظیمات
# =====================================================================
DATA_PATH   = "/opt/airflow/data"
OUTPUT_PATH = "/opt/airflow/output2"
os.makedirs(DATA_PATH,   exist_ok=True)
os.makedirs(OUTPUT_PATH, exist_ok=True)

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = "alireza.abolhassani84@gmail.com"
SMTP_PASS = "darxnbpqenbyvhdk"
TO_EMAIL  = "alireza.abolhassani1384@gmail.com"

# =====================================================================
# رنگ‌ها - حرفه‌ای و زیبا
# =====================================================================
C_TRIGGER = '#2563EB'  # آبی سلطنتی
C_CSM     = '#EF4444'  # قرمز روشن
C_PARDIS  = '#22C55E'  # سبز روشن
C_ANOMALY = '#F59E0B'  # کهربایی
C_OK      = '#10B981'  # سبز زمردی
C_TEXT    = '#1E293B'  # اسلیت تیره
C_SUBTLE  = '#64748B'  # اسلیت متوسط
C_BG      = '#F8FAFC'  # پس‌زمینه روشن
C_EXCELLENT = '#22C55E'
C_GOOD = '#F59E0B'
C_WARNING = '#EF4444'
C_CRITICAL = '#991B1B'
C_HEADER = '#1E293B'
C_BORDER = '#E2E8F0'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =====================================================================
# توابع کمکی
# =====================================================================
def _fa(s):
    return s.translate(str.maketrans("0123456789", "۰۱۲۳۴۵۶۷۸۹"))

def fa(n):
    if n is None or (isinstance(n, float) and pd.isna(n)):
        return "۰"
    if isinstance(n, float) and n == int(n):
        n = int(n)
    return _fa(f"{n:,}")

def normalize_key(name):
    n = str(name).strip()
    if '-' in n and len(n.split('-')[-1]) >= 6:
        base = n.split('-')[0]
    else:
        base = n
    base = base.upper().replace(" ", "").replace("_", "").replace("-", "")
    for s in ["PARDISV2", "PARDIS", "ACTIONPARDIS", "ACTION", "V2", "NID", "NIOD"]:
        base = base.replace(s, "")
    return base

def get_drop_status(drop):
    if drop < 0:
        return "غیرعادی", C_ANOMALY
    elif drop < 20:
        return "عالی", C_EXCELLENT
    elif drop < 40:
        return "خوب", C_GOOD
    elif drop < 60:
        return "متوسط", C_SUBTLE
    elif drop < 80:
        return "ضعیف", C_WARNING
    else:
        return "بحرانی", C_CRITICAL

def format_number_with_commas(n):
    if n is None or (isinstance(n, float) and pd.isna(n)):
        return "۰"
    if isinstance(n, float) and n == int(n):
        n = int(n)
    return _fa(f"{n:,}")

def send_error_email(msg, subj="Error in Dashboard"):
    try:
        body = f"<html><body><h2>{subj}</h2><pre>{msg}</pre></body></html>"
        m = MIMEMultipart('alternative')
        m['From'] = SMTP_USER
        m['To'] = TO_EMAIL
        m['Subject'] = f"{subj} - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        m.attach(MIMEText(body, 'html', 'utf-8'))
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
            s.starttls()
            s.login(SMTP_USER, SMTP_PASS)
            s.send_message(m)
    except Exception as e:
        logger.error(f"Failed to send error email: {e}")

def cleanup_old_files(days=7):
    cutoff = datetime.now() - timedelta(days=days)
    deleted = 0
    for fp in glob.glob(f"{OUTPUT_PATH}/*.html"):
        try:
            if os.path.getctime(fp) < cutoff.timestamp():
                os.remove(fp)
                deleted += 1
        except Exception:
            pass
    return deleted

# =====================================================================
# لودر داده‌ها
# =====================================================================
def _read_csv(path, rename=None):
    if not os.path.exists(path):
        return pd.DataFrame(columns=["day", "logic_name", "sum"])
    df = pd.read_csv(path)
    if rename:
        df = df.rename(columns=rename)
    df["day"] = pd.to_datetime(df["day"])
    df["sum"] = pd.to_numeric(df["sum"], errors='coerce').fillna(0)
    return df

def load_triggered():
    df = _read_csv(f"{DATA_PATH}/Triggered.csv")
    if "logiccomponentid" not in df.columns and "logic_name" in df.columns:
        df = df.rename(columns={"logic_name": "logiccomponentid"})
    df["day"] = pd.to_datetime(df["day"])
    df["sum"] = pd.to_numeric(df["sum"], errors='coerce').fillna(0)
    return df

def load_csm():
    return _read_csv(f"{DATA_PATH}/cms.csv",
                      rename={"action_name": "logic_name", "count": "sum"})

def load_pardis():
    return _read_csv(f"{DATA_PATH}/Pardis.csv",
                      rename={"action_name": "logic_name", "count": "sum"})

# =====================================================================
# ساخت لیست مقایسه‌ای - فقط سرویس‌های فعال (Trigger > 0)
# =====================================================================
def build_items(df_trigger, df_csm, df_pardis):
    trig_sum = df_trigger.groupby("logiccomponentid")["sum"].sum() if not df_trigger.empty else pd.Series(dtype=float)
    trig_sum = trig_sum[trig_sum > 0]
    
    csm_sum = df_csm.groupby("logic_name")["sum"].sum() if not df_csm.empty else pd.Series(dtype=float)
    pardis_sum = df_pardis.groupby("logic_name")["sum"].sum() if not df_pardis.empty else pd.Series(dtype=float)

    def key_map(series):
        d = {}
        for k, v in series.items():
            nk = normalize_key(k)
            d[nk] = (k, d[nk][1] + v if nk in d else v)
        return d

    trig_k = key_map(trig_sum)
    csm_k = key_map(csm_sum)
    pardis_k = key_map(pardis_sum)

    items = []
    for key, (name, tv) in trig_k.items():
        if not tv or tv <= 0:
            continue
        cv = csm_k.get(key, (None, 0))[1]
        pv = pardis_k.get(key, (None, 0))[1]
        combo = cv + pv
        dr = (tv - combo) / tv * 100
        items.append(dict(
            name=name, trig=tv, csm=cv, pardis=pv,
            combo=combo, drop=dr, anomaly=(combo > tv)
        ))
    items.sort(key=lambda x: x["drop"])
    return items

# =====================================================================
# ساخت لیست مقایسه‌ای به تفکیک ماه برای هر سرویس
# =====================================================================
def build_monthly_items_per_service(df_trigger, df_csm, df_pardis):
    if df_trigger.empty:
        return {}
    
    df_t = df_trigger.copy()
    df_t["month"] = df_t["day"].dt.to_period("M")
    
    df_c = df_csm.copy() if not df_csm.empty else pd.DataFrame()
    if not df_c.empty:
        df_c["month"] = df_c["day"].dt.to_period("M")
    
    df_p = df_pardis.copy() if not df_pardis.empty else pd.DataFrame()
    if not df_p.empty:
        df_p["month"] = df_p["day"].dt.to_period("M")
    
    def normalize_series(df, name_col):
        if df.empty:
            return {}
        result = {}
        for _, row in df.iterrows():
            key = normalize_key(row[name_col])
            month = row["month"]
            val = row["sum"]
            if key not in result:
                result[key] = {}
            if month not in result[key]:
                result[key][month] = 0
            result[key][month] += val
        return result
    
    t_dict = normalize_series(df_t, "logiccomponentid")
    c_dict = normalize_series(df_c, "logic_name")
    p_dict = normalize_series(df_p, "logic_name")
    
    all_services = set(t_dict.keys())
    all_months = set()
    for svc in all_services:
        all_months.update(t_dict.get(svc, {}).keys())
        all_months.update(c_dict.get(svc, {}).keys())
        all_months.update(p_dict.get(svc, {}).keys())
    
    all_months = sorted(all_months)
    
    result = {}
    for svc in sorted(all_services):
        result[svc] = {}
        for month in all_months:
            result[svc][month] = {
                'trig': t_dict.get(svc, {}).get(month, 0),
                'csm': c_dict.get(svc, {}).get(month, 0),
                'pardis': p_dict.get(svc, {}).get(month, 0)
            }
    
    return result

# =====================================================================
# 1. کارت‌های KPI حرفه‌ای
# =====================================================================
def kpi_cards_simple(items, df_trigger):
    n_anomaly = sum(1 for it in items if it["anomaly"])
    active_services = len(items)
    
    if items:
        best = min(items, key=lambda x: x["drop"])
        best_status, best_color = get_drop_status(best["drop"])
        best_str = f'{best["name"].split("-")[0][:14]}'
        best_drop = f'{_fa(f"{best['drop']:.1f}")}%'
        
        worst = max(items, key=lambda x: x["drop"])
        worst_status, worst_color = get_drop_status(worst["drop"])
        worst_str = f'{worst["name"].split("-")[0][:14]}'
        worst_drop = f'{_fa(f"{worst['drop']:.1f}")}%'
    else:
        best_str = "—"
        worst_str = "—"
        best_drop = "—"
        worst_drop = "—"
        best_color = C_SUBTLE
        worst_color = C_SUBTLE
    
    cards = [
        {"label": "تعداد سرویس‌های فعال", "value": fa(active_services), "color": C_TRIGGER, "icon": "📊"},
        {"label": "وضعیت غیرعادی", "value": fa(n_anomaly), "color": C_ANOMALY if n_anomaly else C_OK, "icon": "⚠️"},
        {"label": "بهترین سرویس", "value": best_str, "sub": best_drop, "color": best_color, "icon": "🏆"},
        {"label": "بدترین سرویس", "value": worst_str, "sub": worst_drop, "color": worst_color, "icon": "🔻"},
    ]
    
    cells = []
    for card in cards:
        sub_html = f'<div style="font-size:10px;color:{C_SUBTLE};font-weight:normal;">{card.get("sub", "")}</div>' if card.get("sub") else ''
        cells.append(f'''
        <td width="25%" style="padding:5px;">
          <table cellpadding="0" cellspacing="0" border="0" width="100%" style="border-radius:10px;background-color:#ffffff;box-shadow:0 1px 3px rgba(0,0,0,0.06);border:1px solid {C_BORDER};">
            <tr><td height="3" bgcolor="{card['color']}" style="background-color:{card['color']};height:3px;border-radius:10px 10px 0 0;">&nbsp;</td></tr>
            <tr><td align="center" style="padding:10px 8px 4px;">
              <span style="font-size:20px;">{card['icon']}</span>
              <span style="font-size:9px;color:{C_SUBTLE};font-weight:bold;display:block;margin-top:2px;">{card['label']}</span>
            </td></tr>
            <tr><td align="center" style="padding:2px 8px 8px;" dir="ltr">
              <span style="font-size:18px;color:{card['color']};font-weight:bold;">{card['value']}</span>
              {sub_html}
            </td></tr>
          </table>
        </td>''')
    
    return f'<table cellpadding="0" cellspacing="0" border="0" width="100%"><tr>{"".join(cells)}</tr></table>'

# =====================================================================
# 2. جدول Drop Ratio حرفه‌ای
# =====================================================================
def table_drop_ratio(items):
    rows = []
    for it in items[:30]:
        bg = '#FEF2F2' if it["anomaly"] else '#ffffff'
        status, status_color = get_drop_status(it["drop"])
        dr_str = _fa(f"{it['drop']:.1f}") + "٪"
        warn = " ⚠️" if it["anomaly"] else ""

        csm_part = f'<span style="color:{C_CSM};font-weight:bold;">{fa(it["csm"])}</span>' if it["csm"] else f'<span style="color:{C_SUBTLE};">—</span>'
        pardis_part = f'<span style="color:{C_PARDIS};font-weight:bold;">{fa(it["pardis"])}</span>' if it["pardis"] else f'<span style="color:{C_SUBTLE};">—</span>'
        csm_pardis_cell = f'{csm_part} / {pardis_part}'

        rows.append(f'''<tr>
          <td bgcolor="{bg}" style="background-color:{bg};font-size:11px;color:{C_TEXT};padding:6px 10px;border-bottom:1px solid {C_BORDER};text-align:right;" dir="rtl">{it["name"]}{warn}</td>
          <td bgcolor="{bg}" align="center" style="background-color:{bg};font-size:11px;color:{C_TRIGGER};font-weight:bold;padding:6px 10px;border-bottom:1px solid {C_BORDER};">{fa(it["trig"])}</td>
          <td bgcolor="{bg}" style="background-color:{bg};font-size:11px;padding:6px 10px;border-bottom:1px solid {C_BORDER};" dir="ltr">{csm_pardis_cell}</td>
          <td bgcolor="{bg}" align="center" style="background-color:{bg};font-size:11px;color:{status_color};font-weight:bold;padding:6px 10px;border-bottom:1px solid {C_BORDER};">{dr_str}</td>
          <td bgcolor="{bg}" align="center" style="background-color:{bg};font-size:10px;color:{status_color};padding:6px 10px;border-bottom:1px solid {C_BORDER};font-weight:bold;">{status}</td>
        </tr>''')

    header = f'''<tr style="background-color:#F1F5F9;">
      <td style="font-size:10px;color:{C_SUBTLE};font-weight:bold;padding:8px 10px;border-bottom:2px solid {C_BORDER};text-align:right;">نام سرویس</td>
      <td align="center" style="font-size:10px;color:{C_TRIGGER};font-weight:bold;padding:8px 10px;border-bottom:2px solid {C_BORDER};">Triggered</td>
      <td style="font-size:10px;font-weight:bold;padding:8px 10px;border-bottom:2px solid {C_BORDER};" dir="ltr">
        <span style="color:{C_CSM};">CSM</span> / <span style="color:{C_PARDIS};">Pardis</span>
      </td>
      <td align="center" style="font-size:10px;color:{C_SUBTLE};font-weight:bold;padding:8px 10px;border-bottom:2px solid {C_BORDER};">Drop</td>
      <td align="center" style="font-size:10px;color:{C_SUBTLE};font-weight:bold;padding:8px 10px;border-bottom:2px solid {C_BORDER};">وضعیت</td>
    </tr>'''

    note = f'''<div style="font-size:9px;color:{C_SUBTLE};padding:8px 4px 0;text-align:right;" dir="rtl">
        📊 Drop = درصدی از Triggered که در CSM+Pardis دیده نشده است | هرچه کمتر = بهتر
    </div>'''

    return f'''<table cellpadding="0" cellspacing="0" border="0" width="100%" style="border-collapse:collapse;">{header}{"".join(rows)}</table>{note}'''

# =====================================================================
# 3. جدول خلاصه آماری حرفه‌ای
# =====================================================================
def summary_stats(df_trigger, df_csm, df_pardis, items):
    t_total = df_trigger["sum"].sum() if not df_trigger.empty else 0
    c_total = df_csm["sum"].sum() if not df_csm.empty else 0
    p_total = df_pardis["sum"].sum() if not df_pardis.empty else 0
    
    t_mean = df_trigger["sum"].mean() if not df_trigger.empty else 0
    c_mean = df_csm["sum"].mean() if not df_csm.empty else 0
    p_mean = df_pardis["sum"].mean() if not df_pardis.empty else 0
    
    t_max = df_trigger["sum"].max() if not df_trigger.empty else 0
    c_max = df_csm["sum"].max() if not df_csm.empty else 0
    p_max = df_pardis["sum"].max() if not df_pardis.empty else 0
    
    t_active = set()
    if not df_trigger.empty:
        t_active = set(df_trigger[df_trigger["sum"] > 0]["logiccomponentid"].unique())
    
    c_active = set()
    if not df_csm.empty:
        csm_services = set(df_csm["logic_name"].unique())
        csm_normalized = {normalize_key(s): s for s in csm_services}
        t_normalized = {normalize_key(s): s for s in t_active}
        common_keys = set(csm_normalized.keys()) & set(t_normalized.keys())
        c_active = {csm_normalized[k] for k in common_keys}
    
    p_active = set()
    if not df_pardis.empty:
        pardis_services = set(df_pardis["logic_name"].unique())
        pardis_normalized = {normalize_key(s): s for s in pardis_services}
        t_normalized = {normalize_key(s): s for s in t_active}
        common_keys = set(pardis_normalized.keys()) & set(t_normalized.keys())
        p_active = {pardis_normalized[k] for k in common_keys}
    
    t_count = len(t_active)
    c_count = len(c_active)
    p_count = len(p_active)
    
    anomaly_count = sum(1 for it in items if it["anomaly"])
    
    if items:
        avg_drop = sum(it["drop"] for it in items) / len(items)
        min_drop = min(it["drop"] for it in items)
        max_drop = max(it["drop"] for it in items)
    else:
        avg_drop = min_drop = max_drop = 0
    
    rows = f'''
    <tr>
        <td style="font-weight:bold;color:{C_TEXT};padding:6px 10px;border-bottom:1px solid {C_BORDER};">مجموع کل</td>
        <td style="color:{C_TRIGGER};font-weight:bold;padding:6px 10px;border-bottom:1px solid {C_BORDER};" dir="ltr">{format_number_with_commas(t_total)}</td>
        <td style="color:{C_CSM};font-weight:bold;padding:6px 10px;border-bottom:1px solid {C_BORDER};" dir="ltr">{format_number_with_commas(c_total)}</td>
        <td style="color:{C_PARDIS};font-weight:bold;padding:6px 10px;border-bottom:1px solid {C_BORDER};" dir="ltr">{format_number_with_commas(p_total)}</td>
    </tr>
    <tr>
        <td style="font-weight:bold;color:{C_TEXT};padding:6px 10px;border-bottom:1px solid {C_BORDER};">میانگین</td>
        <td style="color:{C_TRIGGER};padding:6px 10px;border-bottom:1px solid {C_BORDER};" dir="ltr">{format_number_with_commas(round(t_mean, 1))}</td>
        <td style="color:{C_CSM};padding:6px 10px;border-bottom:1px solid {C_BORDER};" dir="ltr">{format_number_with_commas(round(c_mean, 1))}</td>
        <td style="color:{C_PARDIS};padding:6px 10px;border-bottom:1px solid {C_BORDER};" dir="ltr">{format_number_with_commas(round(p_mean, 1))}</td>
    </tr>
    <tr>
        <td style="font-weight:bold;color:{C_TEXT};padding:6px 10px;border-bottom:1px solid {C_BORDER};">بیشترین مقدار</td>
        <td style="color:{C_TRIGGER};padding:6px 10px;border-bottom:1px solid {C_BORDER};" dir="ltr">{format_number_with_commas(t_max)}</td>
        <td style="color:{C_CSM};padding:6px 10px;border-bottom:1px solid {C_BORDER};" dir="ltr">{format_number_with_commas(c_max)}</td>
        <td style="color:{C_PARDIS};padding:6px 10px;border-bottom:1px solid {C_BORDER};" dir="ltr">{format_number_with_commas(p_max)}</td>
    </tr>
    <tr>
        <td style="font-weight:bold;color:{C_TEXT};padding:6px 10px;">تعداد سرویس‌های فعال</td>
        <td style="color:{C_TRIGGER};padding:6px 10px;" dir="ltr">{fa(t_count)}</td>
        <td style="color:{C_CSM};padding:6px 10px;" dir="ltr">{fa(c_count)}</td>
        <td style="color:{C_PARDIS};padding:6px 10px;" dir="ltr">{fa(p_count)}</td>
    </tr>
    <tr style="background-color:#F8FAFC;">
        <td style="font-weight:bold;color:{C_TEXT};padding:6px 10px;">میانگین Drop</td>
        <td colspan="3" style="padding:6px 10px;font-weight:bold;color:{C_CRITICAL if avg_drop > 60 else C_EXCELLENT};" dir="ltr">{_fa(f"{avg_drop:.1f}")}%</td>
    </tr>
    <tr>
        <td style="font-weight:bold;color:{C_TEXT};padding:6px 10px;">بهترین Drop</td>
        <td colspan="3" style="padding:6px 10px;font-weight:bold;color:{C_EXCELLENT};" dir="ltr">{_fa(f"{min_drop:.1f}")}%</td>
    </tr>
    <tr>
        <td style="font-weight:bold;color:{C_TEXT};padding:6px 10px;">بدترین Drop</td>
        <td colspan="3" style="padding:6px 10px;font-weight:bold;color:{C_CRITICAL};" dir="ltr">{_fa(f"{max_drop:.1f}")}%</td>
    </tr>
    '''
    
    header = f'''
    <tr style="background-color:#F1F5F9;">
        <td style="font-weight:bold;color:{C_SUBTLE};padding:6px 10px;border-bottom:2px solid {C_BORDER};">معیار</td>
        <td style="font-weight:bold;color:{C_TRIGGER};padding:6px 10px;border-bottom:2px solid {C_BORDER};">Triggered</td>
        <td style="font-weight:bold;color:{C_CSM};padding:6px 10px;border-bottom:2px solid {C_BORDER};">CSM</td>
        <td style="font-weight:bold;color:{C_PARDIS};padding:6px 10px;border-bottom:2px solid {C_BORDER};">Pardis</td>
    </tr>
    '''
    
    note = f'''<div style="font-size:9px;color:{C_SUBTLE};padding:8px 4px 0;text-align:right;" dir="rtl">
        ⚠️ تعداد سرویس‌های غیرعادی: {fa(anomaly_count)} | 
        📊 Drop: هرچه کمتر = بهتر (۰% = بهترین، ۱۰۰% = بدترین)
    </div>'''
    
    return f'''<table cellpadding="0" cellspacing="0" border="0" width="100%" style="border-collapse:collapse;">{header}{rows}</table>{note}'''

# =====================================================================
# 4. نمودار روند ماهانه - فقط آخرین ماه
# =====================================================================
def chart_trend_line(df_trigger, df_csm, df_pardis, max_h=120):
    t_monthly = df_trigger.groupby(df_trigger["day"].dt.to_period("M"))["sum"].sum() if not df_trigger.empty else pd.Series()
    c_monthly = df_csm.groupby(df_csm["day"].dt.to_period("M"))["sum"].sum() if not df_csm.empty else pd.Series()
    p_monthly = df_pardis.groupby(df_pardis["day"].dt.to_period("M"))["sum"].sum() if not df_pardis.empty else pd.Series()
    
    # فقط آخرین ماهی که داده دارد
    all_months = sorted(set(t_monthly.index) | set(c_monthly.index) | set(p_monthly.index))
    if not all_months:
        return "<p style='text-align:center;color:#64748B;padding:20px;'>داده‌ای موجود نیست</p>"
    
    # فقط آخرین ماه را نگه دار
    last_month = all_months[-1]
    all_months = [last_month]
    
    max_val = max([
        t_monthly.get(last_month, 0),
        c_monthly.get(last_month, 0),
        p_monthly.get(last_month, 0)
    ]) or 1
    
    t_val = t_monthly.get(last_month, 0)
    c_val = c_monthly.get(last_month, 0)
    p_val = p_monthly.get(last_month, 0)
    
    t_h = max(int(t_val / max_val * max_h), 3) if t_val > 0 else 0
    c_h = max(int(c_val / max_val * max_h), 3) if c_val > 0 else 0
    p_h = max(int(p_val / max_val * max_h), 3) if p_val > 0 else 0
    
    month_str = last_month.strftime("%Y-%m")
    
    # محاسبه درصدها برای نمایش در کنار ستون‌ها
    total = t_val + c_val + p_val
    if total > 0:
        t_pct = (t_val / total) * 100
        c_pct = (c_val / total) * 100
        p_pct = (p_val / total) * 100
    else:
        t_pct = c_pct = p_pct = 0
    
    # ساخت نمودار با سه ستون بزرگ و واضح
    chart_html = f'''
    <div style="text-align:center;padding:10px 0;">
        <div style="font-size:13px;font-weight:bold;color:{C_TEXT};margin-bottom:12px;" dir="ltr">
            📅 {month_str}
        </div>
        <table cellpadding="0" cellspacing="0" border="0" style="margin:0 auto;">
            <tr>
                <td align="center" style="padding:0 12px;vertical-align:bottom;">
                    <table cellpadding="0" cellspacing="0" border="0">
                        <tr><td align="center" dir="ltr" style="font-size:12px;color:{C_TRIGGER};font-weight:bold;padding-bottom:4px;">{format_number_with_commas(t_val)}</td></tr>
                        <tr><td width="40" height="{t_h}" bgcolor="{C_TRIGGER}" style="background-color:{C_TRIGGER};height:{t_h}px;border-radius:4px 4px 0 0;font-size:0;box-shadow:0 2px 4px rgba(37,99,235,0.3);">&nbsp;</td></tr>
                        <tr><td align="center" style="font-size:10px;color:{C_TRIGGER};padding-top:6px;font-weight:bold;">Triggered</td></tr>
                        <tr><td align="center" style="font-size:9px;color:{C_SUBTLE};">{_fa(f"{t_pct:.1f}")}%</td></tr>
                    </table>
                </td>
                <td align="center" style="padding:0 12px;vertical-align:bottom;">
                    <table cellpadding="0" cellspacing="0" border="0">
                        <tr><td align="center" dir="ltr" style="font-size:12px;color:{C_CSM};font-weight:bold;padding-bottom:4px;">{format_number_with_commas(c_val)}</td></tr>
                        <tr><td width="40" height="{c_h}" bgcolor="{C_CSM}" style="background-color:{C_CSM};height:{c_h}px;border-radius:4px 4px 0 0;font-size:0;box-shadow:0 2px 4px rgba(239,68,68,0.3);">&nbsp;</td></tr>
                        <tr><td align="center" style="font-size:10px;color:{C_CSM};padding-top:6px;font-weight:bold;">CSM</td></tr>
                        <tr><td align="center" style="font-size:9px;color:{C_SUBTLE};">{_fa(f"{c_pct:.1f}")}%</td></tr>
                    </table>
                </td>
                <td align="center" style="padding:0 12px;vertical-align:bottom;">
                    <table cellpadding="0" cellspacing="0" border="0">
                        <tr><td align="center" dir="ltr" style="font-size:12px;color:{C_PARDIS};font-weight:bold;padding-bottom:4px;">{format_number_with_commas(p_val)}</td></tr>
                        <tr><td width="40" height="{p_h}" bgcolor="{C_PARDIS}" style="background-color:{C_PARDIS};height:{p_h}px;border-radius:4px 4px 0 0;font-size:0;box-shadow:0 2px 4px rgba(34,197,94,0.3);">&nbsp;</td></tr>
                        <tr><td align="center" style="font-size:10px;color:{C_PARDIS};padding-top:6px;font-weight:bold;">Pardis</td></tr>
                        <tr><td align="center" style="font-size:9px;color:{C_SUBTLE};">{_fa(f"{p_pct:.1f}")}%</td></tr>
                    </table>
                </td>
            </tr>
        </table>
        <div style="font-size:9px;color:{C_SUBTLE};margin-top:10px;">📊 مقایسه سه بخش در آخرین ماه</div>
    </div>
    '''
    
    return chart_html

# =====================================================================
# 5. نمودار نسبی هر سرویس
# =====================================================================
def chart_relative_pies(items, bar_width=160, bar_height=22):
    if not items:
        return "<p style='text-align:center;color:#64748B;padding:20px;'>داده‌ای موجود نیست</p>"

    rows = []
    for it in items[:20]:
        trig = it["trig"]
        csm = it["csm"]
        pardis = it["pardis"]
        total = trig + csm + pardis

        if total == 0:
            continue

        trig_pct = (trig / total) * 100
        csm_pct = (csm / total) * 100
        pardis_pct = (pardis / total) * 100

        trig_w = max(int(round(trig_pct / 100 * bar_width)), 1) if trig > 0 else 0
        csm_w = max(int(round(csm_pct / 100 * bar_width)), 1) if csm > 0 else 0
        pardis_w = max(int(round(pardis_pct / 100 * bar_width)), 1) if pardis > 0 else 0
        
        diff = bar_width - (trig_w + csm_w + pardis_w)
        if diff != 0:
            max_w = max(trig_w, csm_w, pardis_w)
            if trig_w == max_w:
                trig_w += diff
            elif csm_w == max_w:
                csm_w += diff
            else:
                pardis_w += diff

        short_name = it["name"].split("-")[0][:24]
        if len(it["name"].split("-")[0]) > 24:
            short_name += "..."

        bar_cells = (
            f'<td width="{trig_w}" height="{bar_height}" bgcolor="{C_TRIGGER}" '
            f'style="background-color:{C_TRIGGER};font-size:0;">&nbsp;</td>'
            f'<td width="{csm_w}" height="{bar_height}" bgcolor="{C_CSM}" '
            f'style="background-color:{C_CSM};font-size:0;">&nbsp;</td>'
            f'<td width="{pardis_w}" height="{bar_height}" bgcolor="{C_PARDIS}" '
            f'style="background-color:{C_PARDIS};font-size:0;">&nbsp;</td>'
        )

        trig_label = format_number_with_commas(trig) if trig > 0 else "۰"
        csm_label = format_number_with_commas(csm) if csm > 0 else "۰"
        pardis_label = format_number_with_commas(pardis) if pardis > 0 else "۰"
        
        status, status_color = get_drop_status(it["drop"])
        drop_str = f"Drop: {_fa(f'{it['drop']:.1f}')}%"

        rows.append(f'''
        <tr>
          <td style="font-size:11px;color:{C_TEXT};padding:5px 8px 3px 0;direction:rtl;white-space:nowrap;font-weight:bold;">{short_name}</td>
          <td style="padding:5px 0 3px;">
            <table cellpadding="0" cellspacing="0" border="0" style="border-radius:4px;overflow:hidden;border:1px solid {C_BORDER};">
              <tr>{bar_cells}</tr>
            </table>
          </td>
          <td style="font-size:9px;color:{C_SUBTLE};padding:5px 0 3px 10px;white-space:nowrap;" dir="ltr">
            <span style="color:{C_TRIGGER};">● {trig_label}</span>
            <span style="color:{C_CSM};"> ● {csm_label}</span>
            <span style="color:{C_PARDIS};"> ● {pardis_label}</span>
            <span style="color:{status_color};font-weight:bold;padding-left:8px;">{drop_str}</span>
          </td>
        </tr>''')

    legend = f'''<table cellpadding="0" cellspacing="0" border="0" style="margin:0 auto 12px;">
        <tr>
            <td style="padding-left:16px;"><span style="color:{C_TRIGGER};font-size:11px;">■</span> <span style="font-size:10px;color:{C_TEXT};">Triggered</span></td>
            <td style="padding-left:16px;"><span style="color:{C_CSM};font-size:11px;">■</span> <span style="font-size:10px;color:{C_TEXT};">CSM</span></td>
            <td style="padding-left:16px;"><span style="color:{C_PARDIS};font-size:11px;">■</span> <span style="font-size:10px;color:{C_TEXT};">Pardis</span></td>
            <td style="padding-left:16px;"><span style="color:{C_SUBTLE};font-size:9px;">Drop: هرچه کمتر = بهتر</span></td>
        </tr>
    </table>'''

    return legend + f'<table cellpadding="0" cellspacing="0" border="0" align="center" width="100%">{"".join(rows)}</table>'

# =====================================================================
# 6. نمودار مقایسه‌ای ماهانه
# =====================================================================
def chart_monthly_per_service(service_data, max_h=80):
    if not service_data:
        return "<p style='text-align:center;color:#64748B;padding:20px;'>داده‌ای موجود نیست</p>"
    
    all_months = set()
    for svc, months in service_data.items():
        all_months.update(months.keys())
    all_months = sorted(all_months)
    
    if not all_months:
        return "<p style='text-align:center;color:#64748B;padding:20px;'>داده‌ای موجود نیست</p>"
    
    max_val = 0
    for svc, months in service_data.items():
        for month, data in months.items():
            max_val = max(max_val, data['trig'], data['csm'], data['pardis'])
    max_val = max_val or 1
    
    service_items = list(service_data.items())
    
    month_headers = ''.join([
        f'<td style="font-size:8px;color:{C_SUBTLE};padding:4px 6px;text-align:center;white-space:nowrap;border-bottom:2px solid {C_BORDER};font-weight:bold;" dir="ltr">{m.strftime("%m/%y")}</td>'
        for m in all_months
    ])
    
    rows = []
    for svc, months in service_items:
        short_name = svc[:18]
        if len(svc) > 18:
            short_name += "..."
        
        cells = []
        for month in all_months:
            data = months.get(month, {'trig': 0, 'csm': 0, 'pardis': 0})
            
            th = max(int(data['trig'] / max_val * max_h), 3) if data['trig'] > 0 else 0
            ch = max(int(data['csm'] / max_val * max_h), 3) if data['csm'] > 0 else 0
            ph = max(int(data['pardis'] / max_val * max_h), 3) if data['pardis'] > 0 else 0
            
            c_anomaly = data['csm'] > data['trig']
            p_anomaly = data['pardis'] > data['trig']
            cc = C_ANOMALY if c_anomaly else C_CSM
            pc = C_ANOMALY if p_anomaly else C_PARDIS
            
            cells.append(f'''
            <td style="padding:3px 4px;text-align:center;border-bottom:1px solid {C_BORDER};">
                <table cellpadding="0" cellspacing="0" border="0" style="margin:0 auto;">
                    <tr>
                        <td style="padding:0 2px;text-align:center;">
                            <table cellpadding="0" cellspacing="0" border="0" style="margin:0 auto;">
                                <tr><td align="center" dir="ltr" style="font-size:6px;color:{C_TRIGGER};font-weight:bold;padding-bottom:2px;">{format_number_with_commas(data['trig']) if data['trig'] > 0 else ''}</td></tr>
                                <tr><td width="12" height="{th}" bgcolor="{C_TRIGGER}" style="background-color:{C_TRIGGER};height:{th}px;border-radius:2px 2px 0 0;font-size:0;">&nbsp;</td></tr>
                            </table>
                        </td>
                        <td style="padding:0 2px;text-align:center;">
                            <table cellpadding="0" cellspacing="0" border="0" style="margin:0 auto;">
                                <tr><td align="center" dir="ltr" style="font-size:6px;color:{cc};font-weight:bold;padding-bottom:2px;">{format_number_with_commas(data['csm']) if data['csm'] > 0 else ''}</td></tr>
                                <tr><td width="12" height="{ch}" bgcolor="{cc}" style="background-color:{cc};height:{ch}px;border-radius:2px 2px 0 0;font-size:0;">&nbsp;</td></tr>
                            </table>
                        </td>
                        <td style="padding:0 2px;text-align:center;">
                            <table cellpadding="0" cellspacing="0" border="0" style="margin:0 auto;">
                                <tr><td align="center" dir="ltr" style="font-size:6px;color:{pc};font-weight:bold;padding-bottom:2px;">{format_number_with_commas(data['pardis']) if data['pardis'] > 0 else ''}</td></tr>
                                <tr><td width="12" height="{ph}" bgcolor="{pc}" style="background-color:{pc};height:{ph}px;border-radius:2px 2px 0 0;font-size:0;">&nbsp;</td></tr>
                            </table>
                        </td>
                    </tr>
                </table>
            </td>''')
        
        rows.append(f'''
        <tr style="border-bottom:1px solid {C_BORDER};">
            <td style="font-size:9px;color:{C_TEXT};padding:4px 10px 4px 0;white-space:nowrap;direction:rtl;font-weight:bold;">{short_name}</td>
            {"".join(cells)}
        </tr>''')
    
    legend = f'''<table cellpadding="0" cellspacing="0" border="0" style="margin-bottom:8px;">
        <tr>
            <td style="padding-left:14px;"><span style="color:{C_TRIGGER};font-size:10px;">■</span> <span style="font-size:9px;color:{C_TEXT};">Triggered</span></td>
            <td style="padding-left:14px;"><span style="color:{C_CSM};font-size:10px;">■</span> <span style="font-size:9px;color:{C_TEXT};">CSM</span></td>
            <td style="padding-left:14px;"><span style="color:{C_PARDIS};font-size:10px;">■</span> <span style="font-size:9px;color:{C_TEXT};">Pardis</span></td>
            <td style="padding-left:14px;"><span style="color:{C_ANOMALY};font-size:10px;">■</span> <span style="font-size:9px;color:{C_ANOMALY};">غیرعادی</span></td>
            <td style="padding-left:14px;"><span style="font-size:9px;color:{C_SUBTLE};">({len(service_items)} سرویس)</span></td>
        </tr>
    </table>'''
    
    return legend + f'''
    <div style="overflow-x:auto;max-height:500px;overflow-y:auto;border:1px solid {C_BORDER};border-radius:6px;">
        <table cellpadding="0" cellspacing="0" border="0" style="border-collapse:collapse;width:100%;">
            <tr style="background-color:#F1F5F9;position:sticky;top:0;z-index:10;">
                <td style="font-size:8px;color:{C_SUBTLE};padding:6px 10px 6px 0;font-weight:bold;border-bottom:2px solid {C_BORDER};background-color:#F1F5F9;">سرویس</td>
                {month_headers}
            </tr>
            {"".join(rows)}
        </table>
    </div>'''

# =====================================================================
# 7. هشدارهای خودکار
# =====================================================================
def auto_alerts(items):
    if not items:
        return "<div style='text-align:center;color:#22C55E;font-weight:bold;padding:10px;'>✅ هیچ سرویس فعالی وجود ندارد</div>"
    
    alerts = []
    
    critical = [it for it in items if it["drop"] >= 80]
    for it in critical[:5]:
        alerts.append(f"🔴 سرویس {it['name'].split('-')[0][:22]} دارای drop بسیار بالا {_fa(f'{it['drop']:.1f}')}% (CSM+Pardis به ندرت Triggered را پوشش می‌دهد)")
    
    warning = [it for it in items if 60 <= it["drop"] < 80]
    for it in warning[:5]:
        alerts.append(f"🟡 سرویس {it['name'].split('-')[0][:22]} دارای drop بالا {_fa(f'{it['drop']:.1f}')}% (نیاز به بررسی)")
    
    excellent = [it for it in items if it["drop"] < 20]
    for it in excellent[:3]:
        alerts.append(f"🟢 سرویس {it['name'].split('-')[0][:22]} عملکرد عالی با drop {_fa(f'{it['drop']:.1f}')}% (پوشش کامل)")
    
    anomaly = [it for it in items if it["drop"] < 0]
    for it in anomaly[:3]:
        alerts.append(f"⚠️ سرویس {it['name'].split('-')[0][:22]} دارای drop منفی {_fa(f'{it['drop']:.1f}')}% (CSM+Pardis از Triggered بیشتر است)")
    
    good_count = sum(1 for it in items if it["drop"] < 40)
    bad_count = sum(1 for it in items if it["drop"] >= 60)
    
    if good_count > 0:
        alerts.append(f"ℹ️ {good_count} سرویس با عملکرد خوب (drop کمتر از ۴۰%)")
    if bad_count > 0:
        alerts.append(f"⚠️ {bad_count} سرویس با عملکرد ضعیف (drop بالای ۶۰%)")
    
    if not alerts:
        return "<div style='text-align:center;color:#22C55E;font-weight:bold;padding:10px;'>✅ همه سرویس‌ها در وضعیت عالی هستند</div>"
    
    alerts_html = ''.join([f'<div style="font-size:10px;padding:4px 0;border-bottom:1px solid {C_BORDER};">{alert}</div>' for alert in alerts])
    return f'<div style="background-color:#ffffff;padding:8px 12px;border-radius:6px;">{alerts_html}</div>'

# =====================================================================
# توابع کمکی برای ساخت بخش‌های HTML
# =====================================================================
def box(title, content):
    return f'''
    <table cellpadding="0" cellspacing="0" border="0" width="100%" style="background-color:#ffffff;border-radius:10px;margin-bottom:12px;border:1px solid {C_BORDER};box-shadow:0 1px 3px rgba(0,0,0,0.04);">
      <tr><td style="font-size:13px;font-weight:bold;color:{C_HEADER};padding:12px 16px 8px;border-bottom:1px solid {C_BORDER};">{title}</td></tr>
      <tr><td style="padding:12px 16px;">{content}</td></tr>
    </table>'''

def section(title, subtitle, *content_blocks):
    inner = "".join(content_blocks)
    return f'''
    <table cellpadding="0" cellspacing="0" border="0" width="100%" style="margin-bottom:20px;" dir="rtl">
      <tr><td style="font-size:20px;font-weight:bold;color:{C_HEADER};padding-bottom:4px;">{title}</td></tr>
      <tr><td style="font-size:11px;color:{C_SUBTLE};padding-bottom:12px;" dir="ltr">{subtitle}</td></tr>
      {inner}
    </table>'''

def tr_content(html):
    return f'<tr><td style="padding-bottom:8px;">{html}</td></tr>'

# =====================================================================
# ساخت HTML گزارش کامل
# =====================================================================
def build_trigger_vs_cp(items, df_trigger, df_csm, df_pardis):
    kpi_html = kpi_cards_simple(items, df_trigger)
    drop_table_html = table_drop_ratio(items)
    stats_html = summary_stats(df_trigger, df_csm, df_pardis, items)
    trend_html = chart_trend_line(df_trigger, df_csm, df_pardis)
    relative_pies_html = chart_relative_pies(items)
    service_data = build_monthly_items_per_service(df_trigger, df_csm, df_pardis)
    monthly_compare_html = chart_monthly_per_service(service_data)
    alerts_html = auto_alerts(items)
    
    ts = datetime.now().strftime('%Y-%m-%d %H:%M')
    
    return section(
        "📊 گزارش جامع مقایسه Triggered vs CSM vs Pardis", ts,
        tr_content(kpi_html),
        tr_content(box("📋 جدول Drop Ratio", drop_table_html)),
        tr_content(
            f'''<table cellpadding="0" cellspacing="0" border="0" width="100%"><tr valign="top">
                <td width="50%" style="padding-left:6px;">{box("📊 خلاصه آماری", stats_html)}</td>
                <td width="50%" style="padding-right:6px;">{box("📈 روند ماهانه (آخرین ماه)", trend_html)}</td>
            </tr></table>'''
        ),
        tr_content(box("📊 نمودار نسبی هر سرویس (Triggered vs CSM vs Pardis)", relative_pies_html)),
        tr_content(box("📊 نمودار مقایسه‌ای ماهانه (هر سرویس در یک خط)", monthly_compare_html)),
        tr_content(box("🚨 هشدارهای خودکار", alerts_html)),
    )

# =====================================================================
# تسک‌های Airflow
# =====================================================================
def generate_all_html(**context):
    try:
        logger.info("Generating all HTML dashboards...")
        df_t = load_triggered()
        df_c = load_csm()
        df_p = load_pardis()
        items = build_items(df_t, df_c, df_p)

        html_compare = build_trigger_vs_cp(items, df_t, df_c, df_p)

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        fp = os.path.join(OUTPUT_PATH, f"comparison_{ts}.html")
        with open(fp, 'w', encoding='utf-8') as f:
            f.write(html_compare)

        logger.info(f"Saved: {fp}")

        ti = context['task_instance']
        ti.xcom_push(key='comparison', value=fp)
        return {'comparison': fp}
    except Exception:
        err = traceback.format_exc()
        logger.error(err)
        send_error_email(err, "Error generating dashboards")
        raise

def cleanup_task(**context):
    return cleanup_old_files(7)

def send_email_task(**context):
    try:
        ti = context['task_instance']

        fp = ti.xcom_pull(key='comparison', task_ids='generate_all_html')
        if not fp:
            files = glob.glob(os.path.join(OUTPUT_PATH, "comparison_*.html"))
            fp = max(files, key=os.path.getctime) if files else None

        if fp and os.path.exists(fp):
            with open(fp, 'r', encoding='utf-8') as f:
                h_compare = f.read()
        else:
            h_compare = f'<p style="color:red;">فایل پیدا نشد: {fp}</p>'

        body = f'''<!DOCTYPE html>
<html>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
  <style>
    body,table,td{{font-family:'Segoe UI',Tahoma,Arial,sans-serif!important;}}
    *{{direction:rtl;}}
    body{{background-color:#F1F5F9;margin:0;padding:0;}}
  </style>
</head>
<body style="margin:0;padding:0;background-color:#F1F5F9;">
<table cellpadding="0" cellspacing="0" border="0" width="100%" bgcolor="#F1F5F9">
  <tr><td align="center" style="padding:20px 10px;">
    <table cellpadding="0" cellspacing="0" border="0" width="960" style="max-width:960px;">
      <tr>
        <td style="background:linear-gradient(135deg, #2563EB 0%, #1D4ED8 100%);border-radius:14px 14px 0 0;padding:24px 30px;text-align:center;">
          <div style="font-size:24px;font-weight:bold;color:#ffffff;">📊 گزارش جامع مقایسه‌ای</div>
          <div style="font-size:13px;color:#93C5FD;margin-top:6px;" dir="ltr">{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>
          <div style="font-size:11px;color:#BFDBFE;margin-top:2px;">گزارش خودکار - Apache Airflow</div>
        </td>
      </tr>
      <tr>
        <td bgcolor="#F8FAFC" style="background-color:#F8FAFC;padding:20px;border-radius:0 0 14px 14px;">
          {h_compare}
        </td>
      </tr>
      <tr><td align="center" style="padding:14px;font-size:10px;color:#94A3B8;">این گزارش به‌صورت خودکار توسط Apache Airflow ارسال شده است</td></tr>
    </table>
  </td></tr>
</table>
</body></html>'''

        msg = MIMEMultipart('alternative')
        msg['From'] = SMTP_USER
        msg['To'] = TO_EMAIL
        msg['Subject'] = f"📊 گزارش جامع مقایسه - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        msg.attach(MIMEText(body, 'html', 'utf-8'))

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
            s.starttls()
            s.login(SMTP_USER, SMTP_PASS)
            s.send_message(msg)
        logger.info(f"Email sent to {TO_EMAIL}")
    except Exception:
        err = traceback.format_exc()
        logger.error(err)
        send_error_email(err, "Error sending email")
        raise

# =====================================================================
# DAG
# =====================================================================
default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="powerbi_auto_report_html_v5",
    schedule_interval="0 */8 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=['report', 'dashboard', 'v5', 'comprehensive'],
) as dag:

    gen = PythonOperator(task_id="generate_all_html",
                          python_callable=generate_all_html,
                          provide_context=True)

    clean = PythonOperator(task_id="cleanup_old_files",
                            python_callable=cleanup_task,
                            provide_context=True,
                            trigger_rule="all_done")

    mail = PythonOperator(task_id="send_email",
                           python_callable=send_email_task,
                           provide_context=True,
                           trigger_rule="all_done")

    gen >> clean >> mail
