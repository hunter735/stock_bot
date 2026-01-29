import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from fpdf import FPDF
from fpdf.enums import XPos, YPos
import os
import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import time
from dotenv import load_dotenv

import warnings
warnings.filterwarnings("ignore", category=SyntaxWarning)

# Safe import for Tamil dates
try:
    from tamil.date import datetime as tamil_dt
    tamil_enabled = True
except ImportError:
    tamil_enabled = False
    
load_dotenv()

# --- MASTER CONFIGURATION ---
SENDER_EMAIL = "cselvakumar735@gmail.com"
SENDER_PASSWORD = os.getenv('EMAIL_PASS')

# Define Profiles for both you and your wife
PROFILES = [
    {
        "name": "Selvakumar",
        "receiver": "cselvakumar735@gmail.com",
        "portfolio": {
            'TATAGOLD.NS': {'shares': 5, 'avg_price': 16.1},
        },
        "prefix": "Sfin"  # File name prefix
    },
    {
        "name": "Annalakshmi",
        "receiver": "selvakumarannalakshmi22@gmail.com",
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
                'Date': today, 'Ticker': ticker, 'Shares': info['shares'],
                'Avg': info['avg_price'], 'Live': round(current_price, 2),
                'PL': round(pl, 2), 'PL_Percent': round(pl_percent, 2)
            })
        except Exception as e:
            print(f"Error fetching {ticker}: {e}")
    return pd.DataFrame(data)

def create_visuals(df, prefix):
    # Pie Chart
    plt.figure(figsize=(6, 4))
    plt.pie(df['Shares'] * df['Live'], labels=df['Ticker'], autopct='%1.1f%%', colors=sns.color_palette('pastel'))
    plt.title('Portfolio Distribution')
    plt.savefig(f'{prefix}_pie_chart.png')
    plt.close()

    # Bar Chart
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
    csv_file = f"{prefix}ance.csv"
    pdf_file = f"{prefix}ance_report.pdf"
    
    df.to_csv(csv_file, mode='a', index=False, header=not os.path.exists(csv_file))

    pdf = PortfolioPDF()
    pdf.add_page()
    pdf.set_font('helvetica', 'B', 12)
    pdf.cell(0, 10, f"Report for: {name}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    
    # Table Header
    pdf.set_font('helvetica', 'B', 10)
    cols = ['Date', 'Ticker', 'Qty', 'Avg', 'Live', 'P&L', 'P&L%']
    pdf.set_fill_color(52, 152, 219); pdf.set_text_color(255)
    for col in cols: pdf.cell(27, 10, col, border=1, align='C', fill=True)
    pdf.ln()

    # Table Body
    pdf.set_font('helvetica', '', 9); pdf.set_text_color(0)
    for _, row in df.iterrows():
        color = (0, 128, 0) if row['PL'] >= 0 else (255, 0, 0)
        pdf.set_text_color(*color)
        for item in row.values: pdf.cell(27, 10, str(item), border=1, align='C')
        pdf.ln()

    # Images
    pdf.ln(10)
    pdf.set_text_color(0)
    pdf.image(f'{prefix}_pie_chart.png', x=10, y=pdf.get_y(), w=90)
    pdf.image(f'{prefix}_bar_chart.png', x=105, y=pdf.get_y(), w=90)
    pdf.output(pdf_file)
    return pdf_file

def send_email(receiver, pdf_path, name):
    msg = MIMEMultipart()
    msg['From'], msg['To'], msg['Subject'] = SENDER_EMAIL, receiver, f"Stock Visual Report - {name}"
    msg.attach(MIMEText(f"Hello {name}, your visual portfolio report is attached.", 'plain'))

    with open(pdf_path, "rb") as f:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f"attachment; filename={pdf_path}")
        msg.attach(part)

    with smtplib.SMTP('smtp.gmail.com', 587) as server:
        server.starttls()
        server.login(SENDER_EMAIL, SENDER_PASSWORD)
        server.send_message(msg)

if __name__ == "__main__":
    for person in PROFILES:
        print(f"Processing report for {person['name']}...")
        
        # 1. Get Data
        df = get_portfolio_data(person['portfolio'])
        
        if not df.empty:
            # 2. Visuals
            create_visuals(df, person['prefix'])
            
            # 3. PDF
            report_path = create_pdf_report(df, person['prefix'], person['name'])
            
            # 4. Email
            try:
                send_email(person['receiver'], report_path, person['name'])
                
                # Tamil Date/Time Output
                if tamil_enabled:
                    # இது "புதன்கிழமை, 28 ஜனவரி 2026" என அச்சிடும்
                    print(tamil_dt.now().strftime_ta("%A, %d %B %Y"))
                
                print(datetime.now().strftime("%I:%M:%S %p"))
                print(f"வெற்றி!, {person['name']}-க்கு மின்னஞ்சல் அனுப்பப்பட்டது.\n")

            except Exception as e:
                print(f"மின்னஞ்சல் தோல்வி ({person['name']}): {e}\n")