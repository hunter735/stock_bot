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
import requests
from whatsapp_api_client_python import API
from dotenv import load_dotenv
import warnings

warnings.filterwarnings("ignore", category=SyntaxWarning)

load_dotenv()

# --- MASTER CONFIGURATION ---
SENDER_EMAIL = "cselvakumar735@gmail.com"
SENDER_PASSWORD = os.getenv('EMAIL_PASS')
ID_INSTANCE = os.getenv('ID_INSTANCE')
API_TOKEN = os.getenv('API_TOKEN')
MY_PHONE = os.getenv('MY_WA_PHONE')
WIFE_PHONE = os.getenv('WIFE_WA_PHONE')

PROFILES = [
    {
        "name": "Selvakumar",
        "receiver": "cselvakumar735@gmail.com",
        "wa_phone": MY_PHONE,
        "portfolio": {'TATAGOLD.NS': {'shares': 5, 'avg_price': 16.1}},
        "prefix": "Sfin"
    },
    {
        "name": "Annalakshmi",
        "receiver": "selvakumarannalakshmi22@gmail.com",
        "wa_phone": WIFE_PHONE,
        "portfolio": {
            'TATAGOLD.NS': {'shares': 9, 'avg_price': 13.04},
            'TATSILV.NS': {'shares': 8, 'avg_price': 26.13},
            'SETFGOLD.NS': {'shares': 1, 'avg_price': 116.85}
        },
        "prefix": "Afin"
    }
]
def init_db():
    conn = sqlite3.connect('portfolio_history.db')
    cursor = conn.cursor()
    # இங்கே 'live_price' என்பதற்கு பதில் 'Live' என்றே கொடுத்துவிடுவோம், 
    # அப்போதுதான் Pandas DataFrame-உடன் ஒத்துப்போகும்.
    cursor.execute('''CREATE TABLE IF NOT EXISTS history 
        (Date TEXT, name TEXT, Ticker TEXT, Qty REAL, Live REAL, PL REAL)''')
    conn.commit()
    conn.close()

def save_to_db(df, name):
    conn = sqlite3.connect('portfolio_history.db')
    df_to_save = df.copy()
    df_to_save['name'] = name
    # DataFrame-ல் உள்ள காலம்களும் SQL டேபிளில் உள்ள காலம்களும் சரியாக இருக்க வேண்டும்
    df_to_save[['Date', 'name', 'Ticker', 'Qty', 'Live', 'PL']].to_sql('history', conn, if_exists='append', index=False)
    conn.close()
    print(f"✅ {name} தரவுகள் டேட்டாபேஸில் சேமிக்கப்பட்டது.")

def get_portfolio_data(portfolio):
    data = []
    today = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %H:%M")
    for ticker, info in portfolio.items():
        try:
            stock = yf.Ticker(ticker)
            current_price = stock.history(period='1d')['Close'].iloc[-1]
            cost_basis = info['shares'] * info['avg_price']
            market_val = info['shares'] * current_price
            pl = market_val - cost_basis
            pl_percent = (pl / cost_basis) * 100 if cost_basis != 0 else 0
            
            data.append({
                'Date': today, 'Ticker': ticker, 'Qty': info['shares'],
                'Avg': info['avg_price'], 'Live': round(current_price, 2),
                'PL': round(pl, 2), 'PL_Percent': round(pl_percent, 2)
            })
        except Exception as e:
            print(f"Error fetching {ticker}: {e}")
    return pd.DataFrame(data)

def create_visuals(df, prefix):
    plt.figure(figsize=(6, 4))
    plt.pie(df['Qty'] * df['Live'], labels=df['Ticker'], autopct='%1.1f%%', colors=sns.color_palette('pastel'))
    plt.title('Portfolio Distribution')
    plt.savefig(f'{prefix}_pie_chart.png')
    plt.close()

    plt.figure(figsize=(6, 4))
    sns.barplot(x='Ticker', y='PL', data=df, hue='Ticker', palette='RdYlGn', legend=False)
    plt.axhline(0, color='black', linewidth=0.8)
    plt.title('Profit & Loss (Rs.)')
    plt.savefig(f'{prefix}_bar_chart.png')
    plt.close()

class PortfolioPDF(FPDF):
    def header(self):
        self.set_font('helvetica', 'B', 16)
        self.cell(0, 10, 'Advanced Portfolio Report', align='C', new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(5)

def create_pdf_report(df, prefix, name):
    pdf_file = f"{prefix}_report.pdf"
    pdf = PortfolioPDF()
    pdf.add_page()
    pdf.set_font('helvetica', 'B', 12)
    pdf.cell(0, 10, f"Report for: {name}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    
    pdf.set_font('helvetica', 'B', 10)
    cols = ['Date', 'Ticker', 'Qty', 'Avg', 'Live', 'P&L', 'P&L%']
    pdf.set_fill_color(52, 152, 219)
    pdf.set_text_color(255)
    for col in cols:
        pdf.cell(27, 10, col, border=1, align='C', fill=True)
    pdf.ln()

    pdf.set_font('helvetica', '', 9)
    for _, row in df.iterrows():
        color = (0, 128, 0) if row['PL'] >= 0 else (255, 0, 0)
        pdf.set_text_color(*color)
        pdf.cell(27, 10, str(row['Date']), border=1, align='C')
        pdf.cell(27, 10, str(row['Ticker']), border=1, align='C')
        pdf.cell(27, 10, str(row['Qty']), border=1, align='C')
        pdf.cell(27, 10, str(row['Avg']), border=1, align='C')
        pdf.cell(27, 10, str(row['Live']), border=1, align='C')
        pdf.cell(27, 10, str(row['PL']), border=1, align='C')
        pdf.cell(27, 10, str(row['PL_Percent']), border=1, align='C')
        pdf.ln()

    pdf.ln(10)
    pdf.set_text_color(0)
    pdf.image(f'{prefix}_pie_chart.png', x=10, y=pdf.get_y(), w=90)
    if os.path.exists(f'{prefix}_bar_chart.png'):
        pdf.image(f'{prefix}_bar_chart.png', x=105, y=pdf.get_y(), w=90)
    
    pdf.output(pdf_file)
    return pdf_file

def send_email(receiver, pdf_path, name):
    msg = MIMEMultipart()
    msg['From'], msg['To'], msg['Subject'] = SENDER_EMAIL, receiver, f"Visual Stock Report - {name}"
    msg.attach(MIMEText(f"Hello {name}, your visual portfolio report is attached.", 'plain'))
    with open(pdf_path, "rb") as f:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(f.read()); encoders.encode_base64(part)
        part.add_header('Content-Disposition', f"attachment; filename={os.path.basename(pdf_path)}")
        msg.attach(part)
    with smtplib.SMTP('smtp.gmail.com', 587) as server:
        server.starttls(); server.login(SENDER_EMAIL, SENDER_PASSWORD); server.send_message(msg)

def send_whatsapp_green(wa_phone, name, df, total_pl):
    if not ID_INSTANCE or not API_TOKEN: return
    try:
        green_api = API.GreenApi(ID_INSTANCE, API_TOKEN)
        chat_id = f"{wa_phone}@c.us"
        
        # வாட்ஸ்அப் செய்திக்கான விரிவான தகவல்கள்
        emoji_total = "📈" if total_pl >= 0 else "📉"
        message = f"🔔 *Live Update*\nவணக்கம் {name} {emoji_total}\n\n*பங்கு விவரங்கள்:*\n"
        
        for _, row in df.iterrows():
            ticker = row['Ticker']
            qty = row['Qty']
            pl = row['PL']
            icon = "✅" if pl >= 0 else "❌"
            message += f"• {ticker} ({qty} Qty): {icon} ரூ. {pl:.2f}\n"
        
        status = "மொத்த இலாபம்" if total_pl >= 0 else "மொத்த நஷ்டம்"
        message += f"\n💰 *{status}: ரூ. {total_pl:.2f}*"
        
        green_api.sending.sendMessage(chatId=chat_id, message=message)
        print(f"WhatsApp sent to {name}")
    except Exception as e:
        print(f"WA Error: {e}")

if __name__ == "__main__":
    init_db()
    ist_now = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=5, minutes=30)
    current_hour = ist_now.hour
    current_minute = ist_now.minute
    
    print(f"Current Market Time (IST): {ist_now.strftime('%I:%M %p')}")

    for person in PROFILES:
        print(f"Processing {person['name']}...")
        df = get_portfolio_data(person['portfolio'])
        if not df.empty:
            total_pl = df['PL'].sum()
            save_to_db(df, person['name'])
            send_whatsapp_green(person['wa_phone'], person['name'], df, total_pl)

            is_morning_mail = (current_hour == 9 and 40 <= current_minute <= 59)
            is_evening_mail = (current_hour == 15 and 0 <= current_minute <= 15)

            if is_morning_mail or is_evening_mail:
                create_visuals(df, person['prefix'])
                report_path = create_pdf_report(df, person['prefix'], person['name'])
                try:
                    send_email(person['receiver'], report_path, person['name'])
                    print(f"Email report sent to {person['name']}.")
                except Exception as e:
                    print(f"Email failed: {e}")
