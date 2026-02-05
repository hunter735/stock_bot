import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from fpdf import FPDF
from fpdf.enums import XPos, YPos
from gtts import gTTS
import os
import requests
import google.genai as genai
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
GEMINI_KEY = os.getenv('GEMINI_API_KEY')

# Gemini AI செட்டப்
client = genai.Client(api_key=GEMINI_KEY) if GEMINI_KEY else None

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

def get_ai_news_analysis(name, ticker):
    if not client: return "   ┗ 📰 NEWS: ஆலோசனை தயார் நிலையில் இல்லை.\n"
    try:
        stock = yf.Ticker(ticker)
        news = stock.news[:2] # கடைசி 2 முக்கிய செய்திகள்
        
        if not news:
            return f"   ┗ 📰 NEWS: {ticker} குறித்து இன்று புதிய செய்திகள் ஏதுமில்லை.\n"
        
        titles = [n['title'] for n in news]
        prompt = f"""
        Investor Name: {name}
        Stock: {ticker}
        Latest News: {titles}
        
        மேற்கண்ட செய்திகளை ஆராய்ந்து, ஒரு மனித ஆலோசகர் பேசுவது போல 1 சுருக்கமான வாக்கியத்தில் தமிழில் பதில் கூறவும்.
        இந்த செய்தி பங்கின் விலையை உயர்த்துமா அல்லது குறைக்குமா என்று மட்டும் சொல்லவும்.
        உதாரணம்: "இன்று இந்தச் செய்தியால் உங்கள் {ticker} உயர வாய்ப்புள்ளது."
        """
        
        response = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
        return f"   ┗ 🤖 *செய்தி ஆய்வு:* _{response.text.strip()}_\n"
    except:
        return f"   ┗ 📰 NEWS: செய்திகளை ஆய்வு செய்வதில் பிழை."

def get_rsi_advice(ticker):
    try:
        # கடந்த 14 நாட்களுக்கான தரவை எடுத்தல்
        stock = yf.Ticker(ticker)
        hist = stock.history(period="1mo") # 1 மாத தரவு தேவை
        
        if len(hist) < 14: return "   ┣ 📈 *RSI:* போதுமான தரவு இல்லை\n"

        delta = hist['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs)).iloc[-1]
        
        rsi_val = round(rsi, 1)
        
        if rsi >= 70:
            return f"   ┣ 📉 *RSI:* {rsi_val} (Overbought) - விலை குறைய வாய்ப்பு! ⚠️\n"
        elif rsi <= 30:
            return f"   ┣ 📈 *RSI:* {rsi_val} (Oversold) - இது வாங்குவதற்கான நேரம்! ✅\n"
        else:
            return f"   ┣ 📊 *RSI:* {rsi_val} (Neutral) - சீராக உள்ளது.\n"
    except:
        return "   ┣ 📈 *RSI:* கணக்கிட முடியவில்லை\n"

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
            intrinsic_value = (22.5 * row['eps'] * row['book_value']) ** 0.5
            
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

def get_ai_expert_advice(name, total_pl, df):
    if not client: return "AI ஆலோசனை தற்போது கிடைக்கவில்லை."
    try:
        holdings = ", ".join([f"{r['Ticker']}: ₹{r['PL']}" for _, r in df.iterrows()])
        prompt = f"""
        Investor: {name}
        Total P&L: ₹{total_pl}
        Details: {holdings}

        நீ ஒரு தேர்ந்த இந்திய பங்குச்சந்தை நிபுணர். மேற்கண்ட போர்ட்ஃபோலியோவை ஆய்வு செய்து, 
        தற்போதைய இந்திய சந்தை நிலவரத்தைக் கருத்தில் கொண்டு 2 வரிகளில் தமிழில் ஆலோசனை கூறவும். 
        பங்குகளைத் தக்கவைக்கலாமா (Hold) அல்லது லாபத்தை எடுக்கலாமா (Profit Booking) என்று மட்டும் கூறவும்.
        """
        response = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
        return response.text
    except: return "சந்தையை அவதானித்து முதலீடு செய்யவும்."

def get_market_sentiment_advice():
    try:
        nifty = yf.Ticker("^NSEI")
        # 1. இன்றைய மாற்றத்தைக் கணக்கிடுதல்
        hist_1d = nifty.history(period="1d")
        daily_change = 0
        if not hist_1d.empty:
            daily_change = ((hist_1d['Close'].iloc[-1] - hist_1d['Open'].iloc[0]) / hist_1d['Open'].iloc[0]) * 100

        # 2. 14 நாள் RSI கணக்கிடுதல்
        hist_14d = nifty.history(period="20d")
        delta = hist_14d['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs)).iloc[-1]

        # 3. ஒருங்கிணைந்த முடிவு (Combined Logic)
        # இன்றைய வீழ்ச்சி 1.5% மேல் இருந்தால், RSI என்ன சொன்னாலும் சந்தை பயத்தில் உள்ளது என்றே காட்டப்படும்
        if daily_change <= -1.5:
            return f"📉 *சந்தை இக்கட்டான நிலையில் உள்ளது:* இன்று நிஃப்டி {daily_change:.2f}% சரிந்துள்ளது. அவசரப்பட்டு விற்காதீர்கள், சந்தை சீராகும் வரை காத்திருக்கவும். ⚠️"
        
        if rsi < 30:
            return "😱 *சந்தை அதீத பயத்தில் உள்ளது (Extreme Fear):*\n   ┗ இது தள்ளுபடி விலையில் வாங்குவதற்கு சிறந்த நேரம்! ✅"
        elif rsi > 70:
            return "🤩 *சந்தை அதீத பேராசையில் உள்ளது (Extreme Greed):*\n   ┗ எச்சரிக்கை! இப்போது புதிய முதலீடுகளைத் தவிர்த்து லாபத்தை எடுக்கலாம். ⚠️"
        else:
            return f"⚖️ *சந்தை நிதானமாக உள்ளது (Neutral):*\n   ┗ RSI: {rsi:.1f}. முதலீடுகளைத் தொடரலாம்."
            
    except Exception as e:
        return "⚖️ சந்தை உணர்வுகளை இப்போது கணக்கிட முடியவில்லை."
    
def get_rebalancing_advice(df):
    try:
        # 1. சொத்துக்களைப் பிரித்தல் (Asset Classification)
        # Ticker பெயரில் GOLD அல்லது SILV இருந்தால் அவை Commodity
        commodity_tickers = ['GOLD', 'SILV', 'SETFGOLD', 'TATAGOLD', 'TATSILV']
        
        df['Total_Value'] = df['Qty'] * df['Live']
        
        # Commodity மற்றும் Equity மதிப்புகளைக் கணக்கிடுதல்
        is_commodity = df['Ticker'].str.contains('|'.join(commodity_tickers), case=False)
        comm_val = df[is_commodity]['Total_Value'].sum()
        equity_val = df[~is_commodity]['Total_Value'].sum()
        
        total_portfolio = comm_val + equity_val
        if total_portfolio == 0: return ""

        # 2. தற்போதைய விழுக்காடு
        current_comm_pct = (comm_val / total_portfolio) * 100
        current_equity_pct = (equity_val / total_portfolio) * 100

        # 3. இலக்கு (Target: 50% Commodity | 50% Equity)
        target_pct = 50.0
        threshold = 5.0 # 5% வித்தியாசம் இருந்தால் மட்டும் எச்சரிக்கை

        advice = "⚖️ *போர்ட்ஃபோலியோ சமநிலை (Rebalancing):*\n"
        advice += f"   ┣ தங்கம்/வெள்ளி: {current_comm_pct:.1f}%\n"
        advice += f"   ┣ பங்குகள் (Equity): {current_equity_pct:.1f}%\n"

        # 4. ஆலோசனை வழங்குதல்
        if current_equity_pct > (target_pct + threshold):
            diff_val = total_portfolio * ((current_equity_pct - target_pct) / 100)
            advice += f"   ┗ ⚠️ *அறிவுரை:* பங்குகள் அதிகமாக உள்ளன. ₹{diff_val:,.0f} மதிப்பிற்கு பங்குகளை விற்று தங்கம்/வெள்ளியில் முதலீடு செய்யவும்.\n"
        elif current_comm_pct > (target_pct + threshold):
            diff_val = total_portfolio * ((current_comm_pct - target_pct) / 100)
            advice += f"   ┗ ⚠️ *அறிவுரை:* தங்கம்/வெள்ளி அதிகமாக உள்ளது. ₹{diff_val:,.0f} மதிப்பிற்கு இவற்றை விற்று பங்குகளில் (Equity) முதலீடு செய்யவும்.\n"
        else:
            advice += "   ┗ ✅ உங்கள் சொத்துக்கள் சரியான சமநிலையில் உள்ளன.\n"

        return advice
    except Exception as e:
        return f"Rebalancing Error: {e}"
def get_profit_booking_advice(df):
    try:
        booking_list = []
        for _, r in df.iterrows():
            # முதலீடு செய்த தொகையைக் கணக்கிடுதல்
            invested_val = r['Avg'] * r['Qty']
            # லாப சதவீதத்தைக் கணக்கிடுதல்
            pl_pct = (r['PL'] / invested_val) * 100
            
            # 20% அல்லது அதற்கு மேல் லாபம் இருந்தால்
            if pl_pct >= 20:
                # எவ்வளவு லாபம் கிடைத்துள்ளது என்பதை ரூபாயில் காட்டுதல்
                booking_list.append(
                    f"   ┣ 🚀 *{r['Ticker']}:* {pl_pct:.1f}% லாபம் (₹{r['PL']:,.2f})\n"
                    f"   ┗ ✨ *அறிவுரை:* இலக்கை எட்டியது! லாபத்தை புக் செய்ய ஒரு பகுதியை விற்கலாம்."
                )
        
        if not booking_list:
            return "   ┗ ✅ அனைத்து பங்குகளும் தற்போது ஹோல்டிங்கில் இருக்கலாம்.\n"
        
        return "\n".join(booking_list) + "\n"
    except Exception as e:
        return f"Profit Booking Error: {e}"    
# --- 2. வரி மதிப்பீடு ---
def estimate_tax(buy_date_str, total_pl):
    if total_pl <= 0: return "வரி இல்லை"
    try:
        # சராசரி அடக்க விலையை அடிப்படையாகக் கொண்ட மொத்த லாபம் (total_pl) இங்கு பயன்படுத்தப்படுகிறது
        buy_date = datetime.strptime(buy_date_str, '%Y-%m-%d')
        days = (datetime.now() - buy_date).days
        
        if days < 365:
            # 1 வருடத்திற்குள் - STCG 20%
            tax_amt = total_pl * 0.20
            return f"STCG(20%): ₹{round(tax_amt, 1)}"
        else:
            # 1 வருடத்திற்கு மேல் - LTCG 12.5% (₹1.25L விலக்குக்கு பின்)
            taxable_pl = max(0, total_pl - 125000)
            tax_amt = taxable_pl * 0.125
            return f"LTCG(12.5%): ₹{round(tax_amt, 1)}"
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
            advice += f"   ┣ {percent}% கூடுதல் ({extra_qty} பங்குகள்) வாங்கினால்:*ரூ.{new_avg:.2f}* (📉 -{reduction:.2f})"
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
    try:
        conn = sqlite3.connect('portfolio_history.db')
        cursor = conn.cursor()
        # PRIMARY KEY சேர்ப்பது மிக முக்கியம். இதுதான் Duplicate-ஐத் தடுக்கும்.
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS history (
                Date TEXT,
                Holder TEXT,
                Ticker TEXT,
                Qty REAL,
                Avg_Price REAL,
                Live_Price REAL,
                PL REAL,
                Tax_Est TEXT,
                PRIMARY KEY (Date, Holder, Ticker)
            )
        ''')
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"❌ Init DB Error: {e}")

def save_to_db(df, holder_name):
    try:
        conn = sqlite3.connect('portfolio_history.db')
        cursor = conn.cursor()
        
        # தற்போதைய தேதியை மட்டும் எடுத்தல் (நேரம் தேவையில்லை)
        current_date = datetime.now().strftime('%Y-%m-%d')

        for _, row in df.iterrows():
            # INSERT OR REPLACE: ஏற்கனவே (தேதி, பெயர், டிக்கர்) இருந்தால் விலையை மட்டும் மாற்றும்
            cursor.execute('''
                INSERT OR REPLACE INTO history 
                (Date, Holder, Ticker, Qty, Avg_Price, Live_Price, PL, Tax_Est)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                current_date, 
                holder_name, 
                row['Ticker'], 
                row['Qty'], 
                row['Avg'], 
                row['Live'], 
                row['PL'],
                row.get('Tax_Estimate', '0.0') # Tax விவரத்தையும் சேர்த்துள்ளோம்
            ))
        
        conn.commit()
        conn.close()
        print(f"✅ {holder_name}-ன் தரவுகள் (EOD Logic) டேட்டாபேஸில் புதுப்பிக்கப்பட்டன.")
    except Exception as e:
        print(f"❌ Database Save Error: {e}")
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
        ai_advice = get_ai_expert_advice(name, total_pl, df)
        ist_time = (datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)).strftime('%I:%M %p')
        emoji_main = "🚀" if total_pl >= 0 else "📉"
        market_status = get_market_breadth()
        rebalance_msg = get_rebalancing_advice(df)
        profit_msg = get_profit_booking_advice(df)
        sentiment_msg = get_market_sentiment_advice()

        message = f"🌟 *பங்குச்சந்தை நேரலை அறிக்கை* 🌟\n"
        message += f"━━━━━━━━━━━━━━━━━━\n"
        message += f"👤 *உரிமையாளர்:* {name}\n"
        message += f"⏰ *நேரம்:* {ist_time}\n"
        message += f"━━━━━━━━━━━━━━━━━━\n"

        for _, r in df.iterrows():
            icon = "🟢" if r['PL'] >= 0 else "🔴"
            pl_label = "லாபம்" if r['PL'] >= 0 else "நஷ்டம்"
            pl_display = f"ரூ. {r['PL']:,.2f}"

            # --- மேம்படுத்தப்பட்ட பகுதி: Qty மற்றும் Avg Price சேர்த்தல் ---
            message += f"{icon} *{r['Ticker']}* (Qty: {int(r['Qty'])})\n"
            message += f"   ┣ Avg Price: *₹{r['Avg']:,.2f}*\n"
            message += f"   ┣ Live Price: *₹{r['Live']:,.2f}*\n"
            message += f"   ┣ {pl_label}: *{pl_display}*\n"
            message += f"   ┗ வரி: _{r['Tax_Estimate']}_\n"
            
            # கூடுதல் தகவல்கள் (RSI, News, AI)
            if r.get('RSI_Advice'): 
                message += f"   {r['RSI_Advice']}"
            if r.get('IV_Advice'):
                message += f"   ┗ {r['IV_Advice']}\n"
            if r.get('Avg_Advice') and r['Avg_Advice'].strip():
                message += f"   {r['Avg_Advice']}\n"
            if r.get('AI_News'): 
                message += f"   {r['AI_News']}"
            
            
            message += f"━━━━━━━━━━━━━━━━━━\n"

        status_icon = "💰" if total_pl >= 0 else "⚠️"
        message += f"{status_icon} *இன்றைய மொத்த நிலை:* \n"
        message += f"👉 *ரூ. {total_pl:,.2f}* {emoji_main}\n"
        message += f"━━━━━━━━━━━━━━━━━━\n"
        message += f"🧠 *Emotional Intelligence:* \n{sentiment_msg}\n"
        message += f"━━━━━━━━━━━━━━━━━━\n"
        message += f"🤖 *AI ஆலோசனை:* \n_{ai_advice}_\n"
        message += f"━━━━━━━━━━━━━━━━━━\n"
        message += f"🎯 *லாப வாய்ப்புகள்:* \n{profit_msg}"
        
        if market_status:
            message += f"━━━━━━━━━━━━━━━━━━\n{market_status}\n"
        if rebalance_msg:
            message += f"━━━━━━━━━━━━━━━━━━\n{rebalance_msg}\n"
        if hedge_msg:
            message += f"━━━━━━━━━━━━━━━━━━\n{hedge_msg}\n"
            
        message += f"━━━━━━━━━━━━━━━━━━\n"
        message += f"💡 _தொடர்ந்து முதலீடு செய்யுங்கள்!_"

        green_api.sending.sendMessage(chatId=chat_id, message=message)
        print(f"✅ வாட்ஸ்அப் அறிக்கை {name}-க்கு அனுப்பப்பட்டது.")
    except Exception as e: 
        print(f"WA Error: {e}")
def create_visuals(df, prefix):
    # 1. Pie Chart - போர்ட்ஃபோலியோ பரவல்
    plt.figure(figsize=(6, 4))
    # 'Qty' மற்றும் 'Live' விலையைப் பெருக்கி பங்குகளின் மதிப்பை கணக்கிடுகிறது
    plt.pie(df['Qty'] * df['Live'], labels=df['Ticker'], autopct='%1.1f%%', colors=sns.color_palette('pastel'))
    plt.title('Portfolio Distribution')
    plt.tight_layout()
    plt.savefig(f'{prefix}_pie_chart.png')
    plt.close()

    # 2. Bar Chart - லாப நஷ்ட விவரம்
    plt.figure(figsize=(6, 4))
    # லாபத்திற்கு பச்சை, நஷ்டத்திற்கு சிவப்பு நிறம்
    colors = ['#66bb6a' if x >= 0 else '#ef5350' for x in df['PL']]
    sns.barplot(x='Ticker', y='PL', data=df, palette=colors, hue='Ticker', legend=False)
    plt.axhline(0, color='black', linewidth=0.8)
    plt.title('Profit & Loss (Rs.)')
    plt.ylabel('Amount (Rs)')
    plt.tight_layout()
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
    pdf.set_font('helvetica', '', 10)
    pdf.cell(0, 10, f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.ln(5)
    pdf.set_font('helvetica', 'B', 9)
    pdf.set_fill_color(52, 152, 219) # Blue
    pdf.set_text_color(255, 255, 255) # White
    cols = ['Date', 'Ticker', 'Qty', 'Avg', 'Live', 'P&L', 'P&L%']
    widths = [27, 27, 20, 28, 28, 30, 30]
    for i in range(len(cols)):
        pdf.cell(widths[i], 10, cols[i], border=1, align='C', fill=True)
    pdf.ln()
    pdf.set_text_color(0, 0, 0)
    pdf.set_font('helvetica', '', 9)
    for _, row in df.iterrows():
        color = (0, 128, 0) if row['PL'] >= 0 else (255, 0, 0)
        pdf.set_text_color(*color)
        date_str = str(row['Date']).split(' ')[0]
        pdf.cell(widths[0], 10, date_str, border=1, align='C')
        pdf.cell(widths[1], 10, str(row['Ticker']), border=1, align='C')
        pdf.cell(widths[2], 10, str(row['Qty']), border=1, align='C')
        pdf.cell(widths[3], 10, f"{row['Avg']:,.2f}", border=1, align='C')
        pdf.cell(widths[4], 10, f"{row['Live']:,.2f}", border=1, align='C')
        pdf.cell(widths[5], 10, f"{row['PL']:,.2f}", border=1, align='C')
        p_perc = round(((row['Live'] - row['Avg']) / row['Avg']) * 100, 2)
        pdf.cell(widths[6], 10, f"{p_perc}%", border=1, align='C')
        pdf.ln()
    pdf.ln(10)
    y_pos = pdf.get_y()
    if os.path.exists(f'{prefix}_pie_chart.png'):
        pdf.image(f'{prefix}_pie_chart.png', x=10, y=y_pos, w=90)
    if os.path.exists(f'{prefix}_bar_chart.png'):
        pdf.image(f'{prefix}_bar_chart.png', x=105, y=y_pos, w=90)
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
    init_db()
    ist = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
    
    # 1. Holiday Check
    h_msg = check_holiday_from_csv()
    if h_msg:
        api = API.GreenApi(ID_INSTANCE, API_TOKEN)
        recipients = [{"name": "Selvakumar", "phone": MY_PHONE}, {"name": "Annalakshmi", "phone": WIFE_PHONE}]
        for person in recipients:
            api.sending.sendMessage(chatId=f"{person['phone']}@c.us", message=f"வணக்கம் {person['name']}!\n{h_msg}")
        print("✅ Holiday notification sent.")
        exit()

    # 2. Portfolio Loading
    try:
        p_df_all = pd.read_csv('portfolio.csv')
    except Exception as e:
        print(f"Error: portfolio.csv not found! {e}")
        exit()
    
    holders = [
        {"name": "Selvakumar", "phone": MY_PHONE, "prefix": "Sfin", "email": "cselvakumar735@gmail.com"},
        {"name": "Annalakshmi", "phone": WIFE_PHONE, "prefix": "Afin", "email": "selvakumarannalakshmi22@gmail.com"}
    ]

    # 3. Batch Download - அனைத்து விலைகளையும் ஒரே நேரத்தில் எடுத்தல் (Optimization)
    all_tickers = p_df_all['Ticker'].unique().tolist()
    # 'yfinance' மூலம் ஒரே அழைப்பில் அனைத்து டேட்டாவையும் பெறுதல்
    market_data = yf.download(all_tickers, period='1d')['Close'].iloc[-1].to_dict()

    for p in holders:
        # .copy() பயன்படுத்துவது பாதுகாப்பானது
        u_data = p_df_all[p_df_all['Holder'] == p['name']].copy()
        if u_data.empty: continue

        # Weighted Average லாஜிக்
        u_data['Total_Cost'] = u_data['Qty'] * u_data['Avg_Price']
        u_data_grouped = u_data.groupby('Ticker').agg({
            'Qty': 'sum',
            'Total_Cost': 'sum',
            'Buy_Date': 'min' 
        }).reset_index()
        u_data_grouped['Avg_Price'] = u_data_grouped['Total_Cost'] / u_data_grouped['Qty']

        results = []
        for _, row in u_data_grouped.iterrows():
            ticker = row['Ticker']
            try:
                # Batch டவுன்லோட் செய்த டேட்டாவில் இருந்து விலையை எடுத்தல்
                ltp = round(market_data.get(ticker, 0), 2)
                if ltp == 0: continue # டேட்டா கிடைக்கவில்லை என்றால் தவிர்க்கவும்
                
                pl = round((ltp - row['Avg_Price']) * row['Qty'], 2)
                tax = estimate_tax(row['Buy_Date'], pl)
                
                # ஆலோசனைகள் (இவை உங்கள் பழைய Functions-ஐப் பயன்படுத்தும்)
                avg_adv = get_averaging_advice(row['Qty'], row['Avg_Price'], ltp)
                iv_adv = get_intrinsic_value_advice(ticker, ltp)
                rsi_adv = get_rsi_advice(ticker)
                ai_news = get_ai_news_analysis(p['name'], ticker)

                single_stock_df = pd.DataFrame([{
                    'Ticker': ticker, 'Qty': row['Qty'], 'Avg': row['Avg_Price'], 'PL': pl
                }])
                profit_adv = get_profit_booking_advice(single_stock_df)

                results.append({
                    'Date': ist.strftime("%Y-%m-%d %H:%M"), 
                    'Ticker': ticker, 'Qty': row['Qty'],
                    'Avg': row['Avg_Price'], 'Live': ltp, 'PL': pl, 
                    'Tax_Estimate': tax, 'Avg_Advice': avg_adv, 
                    'IV_Advice': iv_adv, 'Profit_Advice': profit_adv, 
                    'RSI_Advice': rsi_adv, 'AI_News': ai_news
                })
            except Exception as e:
                print(f"Error processing {ticker}: {e}")

        if not results: continue
        
        df_res = pd.DataFrame(results)
        total_pl = df_res['PL'].sum()
        total_val = (df_res['Live'] * df_res['Qty']).sum()
        hedge_msg = get_hedging_advice(total_val)

        # சேமிப்பு மற்றும் அறிக்கைகள்
        save_to_db(df_res, p['name'])
        send_whatsapp_green(p['phone'], p['name'], df_res, total_pl, hedge_msg)

        # 4. Voice Report - Green API ஆப்ஜெக்ட் ஒருமுறை மட்டும்
        try:
            audio_path = create_voice_report(p['name'], total_pl, df_res, p['prefix'])
            green_api = API.GreenApi(ID_INSTANCE, API_TOKEN)
            green_api.sending.sendFileByUpload(
                chatId=f"{p['phone']}@c.us", 
                path=audio_path, 
                fileName=f"{p['name']}_Market_Report.mp3",
                caption="🎤 இன்றைய குரல் அறிக்கை!"
            )
        except Exception as e:
            print(f"Voice Mail Error: {e}")

        # 5. Visual Reports (காலை 9-10 மற்றும் மாலை 3-4 நேரங்களில் மட்டும்)
        if (9 <= ist.hour <= 10) or (15 <= ist.hour <= 16):
            try:
                create_visuals(df_res, p['prefix'])
                pdf_path = create_pdf_report(df_res, p['prefix'], p['name'])
                send_email(p['email'], pdf_path, p['name'])
                print(f"✅ PDF Report emailed to {p['name']}.")
            except Exception as e: 
                print(f"PDF/Email Error: {e}")

print("🏁 Processing Completed Successfully!")