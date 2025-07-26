import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from fastapi import FastAPI
from pydantic import BaseModel
import asyncpg
from pydantic_ai import Agent
from pydantic_ai.models.openai import OpenAIModel
from pydantic_ai.providers.deepseek import DeepSeekProvider
import os
import asyncio
from dotenv import load_dotenv
load_dotenv()
from fastapi.middleware.cors import CORSMiddleware
from fastapi import Body

DB_CONFIG = {
    'user': 'swd_stockintel_user',
    'password': '04e2ERorKhHYJBHjvEC9poakSgcGYW1F',
    'database': 'swd_stockintel_mmm0',
    'host': 'dpg-d1fqcrili9vc739rk3ug-a.singapore-postgres.render.com',
}

app = FastAPI()

# Enable CORS for all origins (adjust origins as needed for production)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def on_startup():
    app.state.db_pool = await asyncpg.create_pool(**DB_CONFIG)
    print("[PERF] DB pool initialized!")

class ChatRequest(BaseModel):
    message: str

class StockRequest(BaseModel):
    symbol: str

# Agent 1: LLM to determine intent and extract code (any type)
intent_model = OpenAIModel(
    'deepseek-chat',
    provider=DeepSeekProvider(api_key=os.getenv('DEEPSEEK_API_KEY'))
)
intent_agent = Agent(
    intent_model,
    system_prompt=(
        'You are an AI assistant. Your job is to extract any code (such as stock code, product code, customer code, transaction code, etc.) from the user\'s message if they are asking about, referring to, or want to analyze a specific code. '
        'If you find a code, return ONLY the code (do not add anything else, no explanation, no quotes, no extra text). '
        'If there is no code, return "OTHER".\n'
        'Examples:\n'
        'User: "Tell me about VCB"\nOutput: VCB\n'
        'User: "analyze the VCB stock code"\nOutput: VCB\n'
        'User: "What is the price of FPT?"\nOutput: FPT\n'
        'User: "Check transaction code TXN12345"\nOutput: TXN12345\n'
        'User: "Give me news about MWG"\nOutput: MWG\n'
        'User: "What is Vinamilk?"\nOutput: VNM\n'
        'User: "Who is the CEO of VCB?"\nOutput: VCB\n'
        'User: "My customer code is KH001, please check"\nOutput: KH001\n'
        'User: "Tell me a joke"\nOutput: OTHER\n'
        'User: "How is the market today?"\nOutput: MARKET\n'
        'User: "Provide me information about the market?"\nOutput: MARKET\n'
    )
)

# Agent 3: LLM to answer other questions
qa_model = OpenAIModel(
    'deepseek-chat',
    provider=DeepSeekProvider(api_key=os.getenv('DEEPSEEK_API_KEY'))
)
qa_agent = Agent(
    qa_model,
    system_prompt=(
        'You are a friendly and concise financial assistant. Answer questions about stocks, codes, markets, and finance clearly and briefly. '
        'If you don\'t know, politely say so. Use a conversational tone.'
    )
)

async def get_stock_info(symbol: str):
    print('[DEBUG] symbol:', symbol)
    pool = app.state.db_pool
    async with pool.acquire() as conn:
        stock_id = await conn.fetchrow('SELECT stock_id FROM stock WHERE symbol=$1', symbol)
        print('[DEBUG] stock_id:', stock_id)
        if not stock_id:
            print('[DEBUG] Không tìm thấy stock_id cho symbol:', symbol)
            return None
        row = await conn.fetchrow('SELECT * FROM post WHERE stock_id=$1', stock_id['stock_id'])
        print('[DEBUG] row:', row)
    return dict(row) if row else None

async def get_market_info():
    print('[DEBUG] Getting market information')
    pool = app.state.db_pool
    async with pool.acquire() as conn:
        # Query for market-level posts (level = 'MARKET')
        row = await conn.fetchrow(
            'SELECT * FROM post WHERE level=$1 ORDER BY created_at DESC LIMIT 1', 
            'MARKET'
        )
        print('[DEBUG] market row:', row)
    return dict(row) if row else None

@app.post("/chat")
async def chat(req: ChatRequest):
    # Agent 1: LLM determines intent/symbol
    intent = (await intent_agent.run(req.message)).output.strip()
    print('[DEBUG] intent:', intent)
    
    if intent == "MARKET":
        # Handle market-related queries
        info = await get_market_info()
        if info:
            return {"answer": f"Market Analysis: {info['content']}"}
        else:
            return {"answer": "No market analysis available at the moment."}
    elif intent != "OTHER":
        # Handle specific stock queries
        info = await get_stock_info(intent)
        if info:
            return {"answer": f"Information about {intent}: {info['content']}"}
        else:
            return {"answer": f"No information found for symbol {intent}."}
    
    # Agent 3: LLM answers other questions
    answer = (await qa_agent.run(req.message)).output
    return {"answer": answer}




