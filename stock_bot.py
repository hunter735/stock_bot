import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from fpdf import FPDF
from fpdf.enums import XPos, YPos
import os
import sqlite3
import smtplib
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from whatsapp_api_client_python import API
from dotenv import load_dotenv
import warnings

# பிழைகளைத் தவிர்க்க
warnings.filterwarnings("ignore", category=SyntaxWarning)
load_dotenv()

# --- CONFIGURATION (GitHub Secrets) ---
SENDER_EMAIL = "cselvakumar735@gmail.com"
SENDER_PASSWORD = os.getenv('EMAIL_PASS')
ID_INSTANCE = os.getenv('ID_INSTANCE')
API_TOKEN = os.getenv('API_TOKEN')
MY_PHONE = os.getenv('MY_WA_PHONE')
WIFE_PHONE = os.getenv('WIFE_WA_PHONE')

# --- 1. அழகான விடுமுறை வாழ்த்து ---
def check_holiday_from_csv():
    try:
        h_df = pd.read_csv('holidays.csv')
        today = (datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)).strftime('%Y-%m-%d')
        match = h_df[h_df['Date'] == today]
        if not match.empty:
            msg = match.iloc[0]['Message']
            return f"✨ *சிறப்பு அறிவிப்பு* ✨\n\n{msg}\n\n🏖️ *இன்று ஓய்வெடுங்கள், மீண்டும் நாளை சந்திப்போம்!*"
    except:
        return None
    return None

# --- 2. வரி மதிப்பீடு ---
def estimate_tax(buy_date_str, pl):
    if pl <= 0: return "வரி இல்லை"
    try:
        buy_date = datetime.strptime(buy_date_str, '%Y-%m-%d')
        days = (datetime.now() - buy_date).days
        if days < 365:
            return f"STCG(20%): ₹{round(pl * 0.20, 1)}"
        else:
            taxable = max(0, pl - 125000)
            return f"LTCG(12.5%): ₹{round(taxable * 0.125, 1)}"
    except:
        return "தேதி பிழை"

# --- 3. தரவுத்தளம் மற்றும் கோப்புகள் ---
def init_db():
    conn = sqlite3.connect('portfolio_history.db')
    cursor = conn.cursor()
    
    # டேபிள் உருவாக்கும்போது Tax_Est சேர்க்கப்பட்டுள்ளது
    cursor.execute('''CREATE TABLE IF NOT EXISTS history 
        (Date TEXT, name TEXT, Ticker TEXT, Qty REAL, Live REAL, PL REAL, Tax_Est TEXT)''')
    
    # ஏற்கனவே உள்ள டேபிளில் Tax_Est இல்லை என்றால் அதைச் சேர்க்கும் பகுதி
    try:
        cursor.execute("ALTER TABLE history ADD COLUMN Tax_Est TEXT DEFAULT '0.0'")
    except sqlite3.OperationalError:
        # காலம் ஏற்கனவே இருந்தால் இந்த Error வரும், அதை நாம் கண்டு கொள்ளத் தேவையில்லை
        pass
        
    conn.commit()
    conn.close()

def save_to_db(df, name):
    conn = sqlite3.connect('portfolio_history.db')
    df_save = df.copy()
    df_save['name'] = name
    # உங்கள் DataFrame-ல் 'Tax_Estimate' என இருப்பதை 'Tax_Est' என மாற்றுகிறோம்
    df_save.rename(columns={'Tax_Estimate': 'Tax_Est'}, inplace=True)
    
    # சரியான வரிசையில் காலம்களைத் தேர்ந்தெடுத்து சேமித்தல்
    df_save[['Date', 'name', 'Ticker', 'Qty', 'Live', 'PL', 'Tax_Est']].to_sql(
        'history', conn, if_exists='append', index=False
    )
    conn.close()

# --- 4. வாட்ஸ்அப் மெசேஜ் டெக்கரேஷன் ---
def send_whatsapp_green(wa_phone, name, df, total_pl):
    try:
        green_api = API.GreenApi(ID_INSTANCE, API_TOKEN)
        chat_id = f"{wa_phone}@c.us"
        ist_time = (datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)).strftime('%I:%M %p')
        emoji_main = "🚀" if total_pl >= 0 else "📉"
        
        message = f"🌟 *பங்குச்சந்தை நேரலை அறிக்கை* 🌟\n"
        message += f"━━━━━━━━━━━━━━━━━━\n"
        message += f"👤 *உரிமையாளர்:* {name}\n"
        message += f"⏰ *நேரம்:* {ist_time}\n"
        message += f"━━━━━━━━━━━━━━━━━━\n\n"

        for _, r in df.iterrows():
            icon = "🟢" if r['PL'] >= 0 else "🔴"
            message += f"{icon} *{r['Ticker']}*\n"
            message += f"   └ லாபம்/நஷ்டம்: *ரூ. {r['PL']:.2f}*\n"
            message += f"   └ வரி: _{r['Tax_Estimate']}_\n\n"

        message += f"━━━━━━━━━━━━━━━━━━\n"
        status_icon = "💰" if total_pl >= 0 else "⚠️"
        message += f"{status_icon} *இன்றைய மொத்த நிலை:* \n"
        message += f"👉 *ரூ. {total_pl:,.2f}* {emoji_main}\n"
        message += f"━━━━━━━━━━━━━━━━━━\n"
        message += f"💡 _தொடர்ந்து முதலீடு செய்யுங்கள்!_"

        green_api.sending.sendMessage(chatId=chat_id, message=message)
    except Exception as e: print(f"WA Error: {e}")

# --- 5. விசுவல்ஸ் மற்றும் ரிப்போர்ட் ---
def create_visuals(df, prefix):
    plt.figure(figsize=(6, 4))
    plt.pie(df['Qty'] * df['Live'], labels=df['Ticker'], autopct='%1.1f%%', colors=sns.color_palette('pastel'))
    plt.title(f'Portfolio - {prefix}')
    plt.savefig(f'{prefix}_pie_chart.png')
    plt.close()

def create_pdf_report(df, prefix, name):
    pdf_file = f"{prefix}_report.pdf"
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font('helvetica', 'B', 14)
    pdf.cell(0, 10, f"Portfolio Report: {name}", align='C', new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.ln(5)
    pdf.set_font('helvetica', 'B', 10)
    for col in ['Ticker', 'Qty', 'Avg', 'Live', 'P&L']: 
        pdf.cell(38, 10, col, border=1, align='C')
    pdf.ln()
    pdf.set_font('helvetica', '', 9)
    for _, r in df.iterrows():
        pdf.cell(38, 10, str(r['Ticker']), border=1)
        pdf.cell(38, 10, str(r['Qty']), border=1)
        pdf.cell(38, 10, str(r['Avg']), border=1)
        pdf.cell(38, 10, str(r['Live']), border=1)
        pdf.cell(38, 10, str(r['PL']), border=1)
        pdf.ln()
    
    if os.path.exists(f'{prefix}_pie_chart.png'):
        pdf.image(f'{prefix}_pie_chart.png', x=55, y=pdf.get_y()+10, w=100)
    
    pdf.output(pdf_file)
    return pdf_file

def send_email(receiver, pdf_path, name):
    msg = MIMEMultipart()
    msg['From'], msg['To'], msg['Subject'] = SENDER_EMAIL, receiver, f"Stock Report - {name}"
    msg.attach(MIMEText(f"Hi {name}, find attached your visual report.", 'plain'))
    with open(pdf_path, "rb") as f:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(f.read()); encoders.encode_base64(part)
        part.add_header('Content-Disposition', f"attachment; filename={os.path.basename(pdf_path)}")
        msg.attach(part)
    with smtplib.SMTP('smtp.gmail.com', 587) as server:
        server.starttls(); server.login(SENDER_EMAIL, SENDER_PASSWORD); server.send_message(msg)

# --- 6. முதன்மைச் செயல்பாடு ---
if __name__ == "__main__":
    h_msg = check_holiday_from_csv()
    if h_msg:
        api = API.GreenApi(ID_INSTANCE, API_TOKEN)
        recipients = [
            {"name": "Selvakumar", "phone": MY_PHONE},
            {"name": "Annalakshmi", "phone": WIFE_PHONE}
        ]
        
        for person in recipients:
            chat_id = f"{person['phone']}@c.us"
            personalized_msg = f"வணக்கம் {person['name']}!\n{h_msg}"
            api.sending.sendMessage(chatId=chat_id, message=personalized_msg)
            print(f"✅ Holiday greeting sent to {person['name']}")
        exit()
        
    init_db()
    ist = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
    
    # CSV கோப்பை வாசித்தல்
    try:
        p_df_all = pd.read_csv('portfolio.csv')
    except Exception as e:
        print(f"Error: portfolio.csv file not found! {e}")
        exit()
    
    holders = [
        {"name": "Selvakumar", "phone": MY_PHONE, "prefix": "Sfin", "email": "cselvakumar735@gmail.com"},
        {"name": "Annalakshmi", "phone": WIFE_PHONE, "prefix": "Afin", "email": "selvakumarannalakshmi22@gmail.com"}
    ]

    for p in holders:
        u_data = p_df_all[p_df_all['Holder'] == p['name']]
        if u_data.empty: continue
        
        results = []
        for _, row in u_data.iterrows():
            ticker = row['Ticker']
            try:
                stock = yf.Ticker(ticker)
                hist = stock.history(period='1d')
                if hist.empty: continue
                ltp = round(hist['Close'].iloc[-1], 2)
                pl = round((ltp - row['Avg_Price']) * row['Qty'], 2)
                tax = estimate_tax(row['Buy_Date'], pl)
                
                results.append({
                    'Date': ist.strftime("%Y-%m-%d %H:%M"), 
                    'Ticker': ticker, 'Qty': row['Qty'],
                    'Avg': row['Avg_Price'], 'Live': ltp, 'PL': pl, 'Tax_Estimate': tax
                })
            except Exception as e:
                print(f"Error fetching {ticker}: {e}")
        
        if not results: continue
        
        df_res = pd.DataFrame(results)
        save_to_db(df_res, p['name'])
        send_whatsapp_green(p['phone'], p['name'], df_res, df_res['PL'].sum())

        # மின்னஞ்சல் அறிக்கை நேரம் (காலை 9:40 அல்லது மாலை 3:30 வரை)
        if (ist.hour == 9 and ist.minute >= 40) or (ist.hour == 15 and ist.minute <= 30):
            create_visuals(df_res, p['prefix'])
            pdf_path = create_pdf_report(df_res, p['prefix'], p['name'])
            try:
                send_email(p['email'], pdf_path, p['name'])
                print(f"📧 Report sent to {p['name']}")
            except Exception as e: 
                print(f"Email Error: {e}")

    print("🏁 Processing Completed Successfully!")