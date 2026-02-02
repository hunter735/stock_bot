import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from fpdf import FPDF
from fpdf.enums import XPos, YPos
from gtts import gTTS
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

def create_voice_report(name, total_pl, df, prefix):
    status = "உயர்ந்துள்ளது" if total_pl >= 0 else "சரிந்துள்ளது"
    
    # சொல்ல வேண்டிய செய்தி (Script)
    script = f"வணக்கம் {name}. இன்றைய பங்குச்சந்தை நிலவரப்படி உங்கள் போர்ட்ஃபோலியோ {abs(total_pl):.2f} ரூபாய் {status}. "
    
    # அதிக லாபம் கொடுத்த பங்கைச் சேர்த்தல்
    top_stock = df.loc[df['PL'].idxmax()]
    if top_stock['PL'] > 0:
        script += f"இன்று அதிகபட்சமாக {top_stock['Ticker']} பங்கு {top_stock['PL']:.2f} ரூபாய் லாபத்தில் உள்ளது. "
    elif top_stock['PL'] < 0:
        script += f"இன்று உங்களின் எல்லா பங்குகளும் நஷ்டத்தில் உள்ளன. இதில் {top_stock['Ticker']} பங்கு மற்றவற்றை விட குறைவான நஷ்டத்தில் உள்ளது. "
    else:
        script += f"இன்று {top_stock['Ticker']} பங்கில் மாற்றமில்லை. "
    sentiment_text = get_market_sentiment_advice()
    if "பயத்தில்" in sentiment_text:
        script += " தற்போது சந்தையில் பலரும் பயத்தில் இருக்கிறார்கள், எனவே இது உங்களுக்கு நல்ல முதலீட்டு வாய்ப்பு. "
    elif "பேராசையில்" in sentiment_text:
        script += " சந்தை இப்போது உச்சத்தில் உள்ளது, எனவே கவனமாக இருங்கள். "
    else:
        script += " சந்தை இப்போது நிதானமாக உள்ளது. "

    script += "தொடர்ந்து முதலீடு செய்யுங்கள். நன்றி!"

    # குரலாக மாற்றுதல் (Tamil Language)
    tts = gTTS(text=script, lang='ta')
    audio_file = f"{prefix}_voice_report.mp3"
    tts.save(audio_file)
    
    return audio_file

def get_market_breadth():
    try:
        nifty = yf.Ticker("^NSEI")
        hist = nifty.history(period="1d")
        if not hist.empty:
            change = hist['Close'].iloc[-1] - hist['Open'].iloc[-0]
            pct = (change / hist['Open'].iloc[0]) * 100
            
            status = "🟢 வலுவாக உள்ளது" if pct > 0 else "🔴 பலவீனமாக உள்ளது"
            return f"📊 *சந்தை (NIFTY 50):*\n   ┗ {status} ({pct:+.2f}%)"
    except:
        return ""
    return ""
def get_intrinsic_value_advice(ticker, current_price):
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        eps = info.get('trailingEps') or info.get('forwardEps')
        book_value = info.get('bookValue') or info.get('priceToBook') # Price to book
        
        if eps and book_value and eps > 0:
            intrinsic_value = (22.5 * eps * book_value) ** 0.5
            
            # தள்ளுபடி (Discount) கணக்கீடு
            if current_price < intrinsic_value:
                discount = ((intrinsic_value - current_price) / intrinsic_value) * 100
                return f"💎 *உண்மையான மதிப்பு (Intrinsic Value):*\n   ┗ இப்போதைய விலை {discount:.1f}% தள்ளுபடியில் உள்ளது (Fair Value: ₹{intrinsic_value:.2f})."
            else:
                overpriced = ((current_price - intrinsic_value) / intrinsic_value) * 100
                return f"⚠️ *எச்சரிக்கை:* உண்மையான மதிப்பை விட {overpriced:.1f}% கூடுதல் விலையில் உள்ளது (Fair Value: ₹{intrinsic_value:.2f})."
    except:
        pass
    return "   ℹ️ *Intrinsic Value:* தரவு கிடைக்கவில்லை\n"

import requests

def get_market_sentiment_advice():
    try:
        # சந்தை உணர்வுகளை அறிய Fear & Greed API அல்லது மாற்று வழியைப் பயன்படுத்தலாம்
        # உதாரணமாக நிஃப்டியின் RSI மற்றும் Volatility வைத்து நாமே கணக்கிடலாம்
        nifty = yf.Ticker("^NSEI")
        hist = nifty.history(period="14d")
        
        # எளிய RSI கணக்கீடு (மனநிலையை அறிய)
        delta = hist['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs)).iloc[-1]

        if rsi < 30:
            return "😱 *சந்தை அதீத பயத்தில் உள்ளது (Extreme Fear):*\n   ┗ அனைவரும் விற்கிறார்கள். இதுவே தள்ளுபடி விலையில் வாங்குவதற்கு சிறந்த நேரம்! ✅"
        elif rsi > 70:
            return "🤩 *சந்தை அதீத பேராசையில் உள்ளது (Extreme Greed):*\n   ┗ எச்சரிக்கை! இப்போது புதிய முதலீடுகளைத் தவிர்த்து லாபத்தை எடுக்கலாம். ⚠️"
        else:
            return "⚖️ *சந்தை நிதானமாக உள்ளது (Neutral):*\n   ┗ முதலீடுகளைத் தொடரலாம்."
    except:
        return "⚖️ சந்தை உணர்வுகளை இப்போது கணக்கிட முடியவில்லை."
    
def get_rebalancing_advice(df):
    try:
        # 1. தற்போதைய மதிப்புகளைக் கணக்கிடுதல்
        # Ticker பெயரில் 'GOLD' அல்லது 'SETFGOLD' இருந்தால் அதைத் தங்கமாகக் கருதுகிறோம்
        df['Total_Value'] = df['Qty'] * df['Live']
        gold_val = df[df['Ticker'].str.contains('GOLD', case=False)]['Total_Value'].sum()
        stock_val = df[~df['Ticker'].str.contains('GOLD', case=False)]['Total_Value'].sum()
        total_portfolio = gold_val + stock_val
        
        if total_portfolio == 0: return ""

        # 2. தற்போதைய விழுக்காடு
        current_gold_pct = (gold_val / total_portfolio) * 100
        current_stock_pct = (stock_val / total_portfolio) * 100
        
        # 3. இலக்கு (Target: Gold 50%, Stocks 50%)
        target_pct = 50.0
        threshold = 5.0 # 5% க்கு மேல் மாற்றம் இருந்தால் மட்டும் எச்சரிக்கை
        
        advice = "⚖️ *போர்ட்ஃபோலியோ சமநிலை (Rebalancing):*\n"
        advice += f"   ┣ தங்கம்: {current_gold_pct:.1f}% | பங்குகள்: {current_stock_pct:.1f}%\n"

        if current_stock_pct > (target_pct + threshold):
            diff_val = total_portfolio * ((current_stock_pct - target_pct) / 100)
            advice += f"   ┗ ⚠️ *அறிவுரை:* பங்குகள் {current_stock_pct:.1f}% ஆக உயர்ந்துள்ளது. ₹{diff_val:,.0f} மதிப்பிற்கு பங்குகளை விற்று (Profit Booking) தங்கத்தில் முதலீடு செய்யவும்.\n"
        elif current_gold_pct > (target_pct + threshold):
            diff_val = total_portfolio * ((current_gold_pct - target_pct) / 100)
            advice += f"   ┗ ⚠️ *அறிவுரை:* தங்கம் {current_gold_pct:.1f}% ஆக உயர்ந்துள்ளது. ₹{diff_val:,.0f} மதிப்பிற்கு தங்கத்தை விற்று பங்குகளில் முதலீடு செய்யவும்.\n"
        else:
            advice += "   ┗ ✅ உங்கள் போர்ட்ஃபோலியோ சரியான சமநிலையில் உள்ளது.\n"
            
        return advice
    except Exception as e:
        return f"Rebalancing Error: {e}"
    
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
    
def get_averaging_advice(current_qty, avg_price, live_price):
    # சந்தை விலை சராசரி விலையை விட 2% கீழ் இருந்தால் மட்டும் ஆலோசனை
    if live_price < (avg_price * 0.98):
        advice = "   📉 *சராசரி செய்ய வாய்ப்பு:*\n"
        for percent in [50, 100]:
            extra_qty = max(1, int(current_qty * (percent / 100)))
            new_avg = ((current_qty * avg_price) + (extra_qty * live_price)) / (current_qty + extra_qty)
            reduction = avg_price - new_avg
            advice += f"   ┣ {percent}% கூடுதல் ({extra_qty} பங்குகள்) வாங்கினால்:*ரூ.{new_avg:.2f}* (📉 -{reduction:.2f})\n"
        return advice
    return ""

def get_hedging_advice(total_portfolio_value):
    try:
        # நிஃப்டி 50 இன் கடந்த 5 நாள் தரவை ஆய்வு செய்தல்
        nifty = yf.Ticker("^NSEI")
        hist = nifty.history(period="5d")
        
        if len(hist) < 2: return "✅ சந்தை தரவு போதிய அளவில் இல்லை."

        start_price = hist['Close'].iloc[0]
        current_price = hist['Close'].iloc[-1]
        market_change = ((current_price - start_price) / start_price) * 100

        # சந்தை 2% க்கும் மேல் சரிந்தால் ஹெட்ஜிங் ஆலோசனை வழங்குதல்
        if market_change < -2.0:
            hedge_amount = total_portfolio_value * 0.15  # 15% ஹெட்ஜிங்
            return (f"🛡️ *அல்காரிதமிக் ஹெட்ஜிங் கவசம்:*\n"
                    f"   ┣ சந்தை கடந்த வாரத்தில் {market_change:.1f}% சரிந்துள்ளது.\n"
                    f"   ┗ ⚠️ *பாதுகாப்பு நடவடிக்கை:* உங்கள் போர்ட்ஃபோலியோவை பாதுகாக்க "
                    f"₹{hedge_amount:,.0f} மதிப்பிற்கு Gold ETF அல்லது Liquid Case வாங்கவும்.")
        
        return "✅ சந்தை சீராக உள்ளது. ஹெட்ஜிங் தேவையில்லை."
    except Exception as e:
        return f"Hedging Error: {e}"
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
def send_whatsapp_green(wa_phone, name, df, total_pl, hedge_msg):
    try:
        green_api = API.GreenApi(ID_INSTANCE, API_TOKEN)
        chat_id = f"{wa_phone}@c.us"
        ist_time = (datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)).strftime('%I:%M %p')
        emoji_main = "🚀" if total_pl >= 0 else "📉"
        market_status = get_market_breadth()
        rebalance_msg = get_rebalancing_advice(df)
        sentiment_msg = get_market_sentiment_advice()

        message = f"🌟 *பங்குச்சந்தை நேரலை அறிக்கை* 🌟\n"
        message += f"━━━━━━━━━━━━━━━━━━\n"
        message += f"👤 *உரிமையாளர்:* {name}\n"
        message += f"⏰ *நேரம்:* {ist_time}\n"
        message += f"━━━━━━━━━━━━━━━━━━\n\n"

        for _, r in df.iterrows():
            icon = "🟢" if r['PL'] >= 0 else "🔴"
            pl_label = "லாபம்" if r['PL'] >= 0 else "நஷ்டம்"
            
            message += f"{icon} *{r['Ticker']}*\n"
            message += f"   ┣ {pl_label}: *ரூ. {abs(r['PL']):.2f}*\n"
            message += f"   ┗ வரி: _{r['Tax_Estimate']}_\n"
            # Intrinsic Value இங்கே வரும்
            if r.get('IV_Advice') and r['IV_Advice'].strip():
                message += r['IV_Advice']
                
            # Averaging Advice இங்கே வரும்
            if r.get('Avg_Advice') and r['Avg_Advice'].strip():
                message += r['Avg_Advice']
            
            message += "\n" # ஒவ்வொரு பங்கிற்கும் இடையில் இடைவெளி

        message += f"━━━━━━━━━━━━━━━━━━\n"
        status_icon = "💰" if total_pl >= 0 else "⚠️"
        message += f"{status_icon} *இன்றைய மொத்த நிலை:* \n"
        message += f"👉 *ரூ. {total_pl:,.2f}* {emoji_main}\n"
        message += f"━━━━━━━━━━━━━━━━━━\n"
        message += f"🧠 *எமோஷனல் இன்டெலிஜென்ஸ்:* \n{sentiment_msg}\n"
        message += f"━━━━━━━━━━━━━━━━━━\n"
        if market_status:
            message += market_status + "\n"
            message += f"━━━━━━━━━━━━━━━━━━\n"
        if rebalance_msg:
            message += rebalance_msg + "\n"
            message += f"━━━━━━━━━━━━━━━━━━\n"
            message += f"{hedge_msg}\n" # ஹெட்ஜிங் மெசேஜ் இங்கே வரும்
            message += f"━━━━━━━━━━━━━━━━━━\n"  
        message += f"💡 _தொடர்ந்து முதலீடு செய்யுங்கள்!_"

        green_api.sending.sendMessage(chatId=chat_id, message=message)
    except Exception as e: 
        print(f"WA Error: {e}")

# --- 5. விசுவல்ஸ் மற்றும் ரிப்போர்ட் ---
def create_visuals(df, prefix):
    # உருவப்படத்தின் அளவை அதிகரித்தல் (Pie + Bar ஆகிய இரண்டிற்கும்)
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
    
    # 1. Pie Chart (Portfolio Distribution)
    ax1.pie(df['Qty'] * df['Live'], labels=df['Ticker'], autopct='%1.1f%%', colors=sns.color_palette('pastel'))
    ax1.set_title(f'Portfolio Distribution')
    
    # 2. Bar Chart (Profit & Loss)
    # லாபம் என்றால் பச்சை, நஷ்டம் என்றால் சிவப்பு நிறம்
    colors = ['#66bb6a' if x >= 0 else '#ef5350' for x in df['PL']]
    ax2.bar(df['Ticker'], df['PL'], color=colors)
    ax2.set_title('Profit & Loss (Rs.)')
    ax2.set_ylabel('Rs')
    
    plt.tight_layout()
    plt.savefig(f'{prefix}_visuals.png') # பெயர் மாற்றம் செய்யப்பட்டுள்ளது
    plt.close()

def create_pdf_report(df, prefix, name):
    pdf_file = f"{prefix}_report.pdf"
    pdf = FPDF()
    pdf.add_page()
    
    # தலைப்பு (Advanced Title)
    pdf.set_font('helvetica', 'B', 16)
    pdf.cell(0, 10, "Advanced Portfolio Report", align='C', new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font('helvetica', 'B', 12)
    pdf.cell(0, 10, f"Report for: {name}", align='L', new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.ln(5)
    
    # டேபிள் தலைப்பு (Header with Colors)
    pdf.set_fill_color(52, 152, 219) # Blue color
    pdf.set_text_color(255, 255, 255) # White text
    pdf.set_font('helvetica', 'B', 9)
    
    cols = ['Date', 'Ticker', 'Qty', 'Avg', 'Live', 'P&L', 'P&L%']
    widths = [25, 30, 15, 25, 25, 25, 25] # ஒவ்வொரு காலமிற்கும் அளவு
    
    for i in range(len(cols)):
        pdf.cell(widths[i], 10, cols[i], border=1, align='C', fill=True)
    pdf.ln()
    
    # டேட்டா வரிகள்
    pdf.set_text_color(0, 0, 0) # Black text for rows
    pdf.set_font('helvetica', '', 8)
    
    for _, r in df.iterrows():
        # P&L% கணக்கீடு
        pandl_perc = round(((r['Live'] - r['Avg']) / r['Avg']) * 100, 2)
        
        # தேதியை மட்டும் எடுத்தல் (நேரம் தவிர்த்து)
        display_date = r['Date'].split(' ')[0]
        
        pdf.cell(widths[0], 10, display_date, border=1, align='C')
        pdf.cell(widths[1], 10, str(r['Ticker']), border=1, align='C')
        pdf.cell(widths[2], 10, str(r['Qty']), border=1, align='C')
        pdf.cell(widths[3], 10, str(r['Avg']), border=1, align='C')
        pdf.cell(widths[4], 10, str(r['Live']), border=1, align='C')
        
        # P&L நிறம் (சிவப்பு/கருப்பு)
        if r['PL'] < 0: pdf.set_text_color(255, 0, 0)
        pdf.cell(widths[5], 10, str(r['PL']), border=1, align='C')
        pdf.cell(widths[6], 10, f"{pandl_perc}%", border=1, align='C')
        pdf.set_text_color(0, 0, 0) # Reset to black
        pdf.ln()

    # வரைபடத்தை சேர்த்தல்
    if os.path.exists(f'{prefix}_visuals.png'):
        pdf.image(f'{prefix}_visuals.png', x=10, y=pdf.get_y() + 10, w=190)

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
                avg_adv = get_averaging_advice(row['Qty'], row['Avg_Price'], ltp)
                iv_adv = get_intrinsic_value_advice(ticker, ltp)
                
                results.append({
                    'Date': ist.strftime("%Y-%m-%d %H:%M"), 
                    'Ticker': ticker, 'Qty': row['Qty'],
                    'Avg': row['Avg_Price'], 'Live': ltp, 'PL': pl, 'Tax_Estimate': tax,'Avg_Advice': avg_adv, 'IV_Advice': iv_adv
                })
            except Exception as e:
                print(f"Error fetching {ticker}: {e}")
        
        if not results: continue
        
        df_res = pd.DataFrame(results)
        total_val = (df_res['Live'] * df_res['Qty']).sum()
        hedge_msg = get_hedging_advice(total_val)
        total_pl = df_res['PL'].sum()
        save_to_db(df_res, p['name'])
        send_whatsapp_green(p['phone'], p['name'], df_res, df_res['PL'].sum(), hedge_msg)
        try:
            audio_path = create_voice_report(p['name'], total_pl, df_res, p['prefix'])
            
            # Green API மூலம் ஆடியோவை அனுப்புதல்
            green_api = API.GreenApi(ID_INSTANCE, API_TOKEN) # green_api ஆப்ஜெக்ட் இங்கே இருப்பதை உறுதி செய்யவும்
            
            green_api.sending.sendFileByUpload(
                chatId=f"{p['phone']}@c.us", 
                path=audio_path, 
                fileName=f"{p['name']}_Market_Report.mp3",
                caption="🎤 இன்றைய குரல் அறிக்கை!"
            )
            print(f"🎙️ Voice report sent to {p['name']}")
        except Exception as e:
            print(f"Voice Mail Error: {e}")

        # மின்னஞ்சல் அறிக்கை நேரம் (காலை 9:40 அல்லது மாலை 3:30 வரை)
        if (9 <= ist.hour <= 10) or (15 <= ist.hour <= 16):
            create_visuals(df_res, p['prefix'])
            pdf_path = create_pdf_report(df_res, p['prefix'], p['name'])
            try:
                send_email(p['email'], pdf_path, p['name'])
                print(f"📧 Report sent to {p['name']}")
            except Exception as e: 
                print(f"Email Error: {e}")
    print("🏁 Processing Completed Successfully!")