import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from fpdf import FPDF
from fpdf.enums import XPos, YPos
import os
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

def get_portfolio_data(portfolio):
    data = []
    today = datetime.now().strftime("%Y-%m-%d")
    for ticker, info in portfolio.items():
        try:
            stock = yf.Ticker(ticker)
            current_price = stock.fast_info['last_price']
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

# ... (முந்தைய கோப்பில் உள்ள மற்ற அனைத்து பகுதிகளும் அப்படியே இருக்கட்டும்) ...

if __name__ == "__main__":
    # IST நேரத்தை UTC-யிலிருந்து கணக்கிடுதல்
    ist_now = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=5, minutes=30)
    current_hour = ist_now.hour
    current_minute = ist_now.minute
    
    print(f"Current Market Time (IST): {ist_now.strftime('%I:%M %p')}")

    for person in PROFILES:
        print(f"Processing {person['name']}...")
        df = get_portfolio_data(person['portfolio'])
        
        if not df.empty:
            total_pl = df['PL'].sum()
            
            # 1. வாட்ஸ்அப் (அனைத்து ரன்களிலும் செல்லும்: 9:20 முதல் 3:10 வரை)
            send_whatsapp_green(person['wa_phone'], person['name'], df, total_pl)

            # 2. மின்னஞ்சல் நேரம்: 
            # காலை 9:45 (9 AM மற்றும் 45 - 55 நிமிடங்களுக்குள்) 
            # அல்லது மாலை 3:00 (15 PM மற்றும் 0 - 10 நிமிடங்களுக்குள்)
            is_morning_mail = (current_hour == 9 and 40 <= current_minute <= 59)
            is_evening_mail = (current_hour == 15 and 0 <= current_minute <= 10)

            if is_morning_mail or is_evening_mail:
                create_visuals(df, person['prefix'])
                report_path = create_pdf_report(df, person['prefix'], person['name'])
                try:
                    send_email(person['receiver'], report_path, person['name'])
                    print(f"Email report sent to {person['name']}.")
                except Exception as e:
                    print(f"Email failed: {e}")