from db_utils.pg_pool import get_pool
from datetime import datetime, time
import aiohttp
import aiofiles
import os
from pathlib import Path
from typing import List, Optional

async def save_support_output_to_db(support_output):
    """
    Lưu thông tin SupportOutput vào bảng stock_analysis, kèm phiên giao dịch.
    support_output: object có các thuộc tính analysis_advice, symbol, sentiment, topic
    session: 'morning', 'afternoon', hoặc 'out_of_session'
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        # Lấy stock_id từ bảng stock
        stock_id = await conn.fetchrow("SELECT stock_id FROM stock WHERE symbol = $1", support_output.symbol)
        if not stock_id:
            raise ValueError(f"Stock with symbol {support_output.symbol} not found")
        
        stock_id = stock_id["stock_id"]
        
        # Lấy session từ hàm get_trading_session
        session = get_trading_session()

        # Lưu thông tin vào bảng post
        await conn.execute(
            """
            INSERT INTO post (title, content, stock_id, sentiment, topic, session, level)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
            support_output.title,
            support_output.analysis_advice,
            stock_id,
            support_output.sentiment,
            support_output.topic,
            session,
            "SYMBOL"
        )

def get_trading_session(dt=None):
    if dt is None:
        dt = datetime.now()
    t = dt.time()
    if t < time(15, 0):
      return 1
    elif time(15, 0) <= t < time(17, 0):
      return 2
    else:
      return 3

async def save_market_analysis_to_db(market_analysis):
    """
    Lưu thông tin MarketAnalysis vào bảng post
    market_analysis: object có các thuộc tính title, analysis, sentiment
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO post (title, content, sentiment, topic, level)
            VALUES ($1, $2, $3, $4, $5)
            """,
            market_analysis.title,
            market_analysis.analysis,
            market_analysis.sentiment,
            "MARKET", 
            "MARKET",
        )

# ============ PODCAST UPLOAD FUNCTIONS ============

async def upload_single_podcast_file(
    server_url: str,
    file_path: str,
    title: str,
    secret_key: str,
    endpoint: str = "/podcasts/upload",
    description: str = None,
    uploaded_by: str = None,
    status: str = "published",
    tags: List[str] = None,
    headers: Optional[dict] = None
) -> dict:
    """
    Upload một file podcast đơn lẻ lên server
    
    Args:
        server_url: URL của server
        file_path: Đường dẫn tới file audio
        title: Tiêu đề podcast (bắt buộc)
        secret_key: Secret key để authentication (bắt buộc)
        endpoint: API endpoint để upload (mặc định: "/podcasts/upload")
        description: Mô tả podcast (tùy chọn)
        uploaded_by: ID người upload (tùy chọn)
        status: Trạng thái podcast (mặc định: "published")
        tags: Tags cho podcast (tùy chọn)
        headers: Headers tùy chọn
    
    Returns:
        dict: Response từ server
    """
    file_path = Path(file_path)
    
    if not file_path.exists():
        raise FileNotFoundError(f"Không tìm thấy file audio '{file_path}'")
    
    url = f"{server_url.rstrip('/')}{endpoint}"
    
    async with aiohttp.ClientSession() as session:
        data = aiohttp.FormData()
        
        # Thêm file audio (bắt buộc)
        async with aiofiles.open(file_path, 'rb') as f:
            file_content = await f.read()
            data.add_field(
                'audio',
                file_content,
                filename=file_path.name,
                content_type='audio/mpeg' if file_path.suffix.lower() == '.mp3' else 'audio/wav'
            )
        
        # Thêm các field bắt buộc
        data.add_field('title', title)
        data.add_field('secretKey', secret_key)
        
        # Thêm các field tùy chọn
        if description:
            data.add_field('description', description)
        if uploaded_by:
            data.add_field('uploadedBy', uploaded_by)
        if status:
            data.add_field('status', status)
        if tags:
            for tag in tags:
                data.add_field('tags', tag)
        
        try:
            print(f"🚀 Đang upload file: {file_path.name}")
            
            async with session.post(url, data=data, headers=headers) as response:
                response_text = await response.text()
                
                try:
                    response_data = await response.json()
                except:
                    response_data = {"message": response_text}
                
                if response.status == 200 or response.status == 201:
                    print(f"✅ Upload thành công {file_path.name}")
                    return {
                        'success': True,
                        'status_code': response.status,
                        'uploaded_file': file_path.name,
                        'title': title,
                        'server_response': response_data
                    }
                else:
                    print(f"❌ Upload thất bại cho {file_path.name} - Status: {response.status}")
                    return {
                        'success': False,
                        'status_code': response.status,
                        'error': response_data,
                        'failed_file': file_path.name
                    }
                    
        except Exception as e:
            print(f"❌ Lỗi khi upload {file_path.name}: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'failed_file': file_path.name
            }

async def upload_audio_directory(
    directory_path: str, 
    secret_key: str, 
    server_url: str = "https://swd-stockintel.onrender.com"
) -> dict:
    """
    HÀM CHÍNH: Upload file audio trong thư mục (mỗi thư mục có 1 file)
    
    Args:
        directory_path: Đường dẫn thư mục chứa file audio
        secret_key: Secret key để authentication
        server_url: URL server
    
    Returns:
        dict: Kết quả upload với thông tin chi tiết
    """
    directory = Path(directory_path)
    
    if not directory.exists():
        raise FileNotFoundError(f"Không tìm thấy thư mục '{directory_path}'")
    
    if not directory.is_dir():
        raise ValueError(f"'{directory_path}' không phải là thư mục")
    
    # Các định dạng audio được hỗ trợ
    audio_extensions = {'.mp3', '.wav', '.m4a', '.aac', '.ogg', '.flac'}
    audio_files = [
        f for f in directory.iterdir() 
        if f.is_file() and f.suffix.lower() in audio_extensions
    ]
    
    if not audio_files:
        return {
            'success': False,
            'error': f"Không tìm thấy file audio nào trong thư mục '{directory_path}'",
            'directory_path': directory_path,
            'uploaded_file': None
        }
    
    # Lấy file đầu tiên (vì mỗi thư mục chỉ có 1 file)
    audio_file = audio_files[0]
    
    if len(audio_files) > 1:
        print(f"⚠️ Cảnh báo: Tìm thấy {len(audio_files)} file, chỉ upload file đầu tiên: {audio_file.name}")
    
    print(f"📁 Đang upload file: {audio_file.name} từ thư mục: {directory_path}")
    
    try:
        # Tạo title từ tên file
        file_name_without_ext = audio_file.stem
        title = f"Podcast: {file_name_without_ext}"
        
        # Tự động phát hiện tags từ tên file và thư mục
        tags = []
        
        # Thêm symbol nếu có trong tên file
        if '_' in file_name_without_ext:
            potential_symbol = file_name_without_ext.split('_')[0].upper()
            if len(potential_symbol) >= 2 and len(potential_symbol) <= 5:
                tags.append(potential_symbol)
        
        # Thêm session hiện tại và ngày
        tags.append(f"session_{get_trading_session()}")
        tags.append(datetime.now().strftime('%Y%m%d'))
        
        # Phát hiện loại phân tích từ tên file
        file_name_lower = file_name_without_ext.lower()
        if 'market' in file_name_lower:
            tags.extend(['MARKET', 'ANALYSIS'])
            description = f"Phân tích thị trường - {datetime.now().strftime('%d/%m/%Y %H:%M')}"
        elif 'analysis' in file_name_lower:
            tags.extend(['STOCK', 'ANALYSIS'])
            description = f"Phân tích cổ phiếu - {datetime.now().strftime('%d/%m/%Y %H:%M')}"
        else:
            tags.append('GENERAL')
            description = f"Podcast được tạo từ Stock AI Tool - {datetime.now().strftime('%d/%m/%Y %H:%M')}"
        
        # Upload file
        result = await upload_single_podcast_file(
            server_url=server_url,
            file_path=str(audio_file),
            title=title,
            secret_key=secret_key,
            description=description,
            uploaded_by="stock-ai-tool",
            status="published",
            tags=tags
        )
        
        if result['success']:
            print(f"✅ Upload thành công: {audio_file.name}")
            return {
                'success': True,
                'directory_path': directory_path,
                'uploaded_file': {
                    'filename': audio_file.name,
                    'filepath': str(audio_file),
                    'title': title,
                    'tags': tags,
                    'server_response': result['server_response']
                },
                'status_code': result['status_code']
            }
        else:
            print(f"❌ Upload thất bại: {audio_file.name}")
            return {
                'success': False,
                'directory_path': directory_path,
                'uploaded_file': None,
                'error': result.get('error', 'Unknown error'),
                'status_code': result.get('status_code', 0)
            }
        
    except Exception as e:
        error_msg = f"Lỗi khi upload {audio_file.name}: {str(e)}"
        print(f"❌ {error_msg}")
        return {
            'success': False,
            'directory_path': directory_path,
            'uploaded_file': None,
            'error': error_msg
        }

# ============ TEST FUNCTION ============
if __name__ == "__main__":
    import asyncio
    
    async def test_upload():
        """
        Test upload thư mục podcast
        """
        try:
            # Tạo file test nếu không có
            print("🔄 Tạo file test...")
            os.makedirs("output/audios/test", exist_ok=True)
            test_file = "output/audios/test/test_podcast.mp3"
            
            if not os.path.exists(test_file):
                from gtts import gTTS
                test_text = "Đây là file test podcast để kiểm tra upload function"
                tts = gTTS(test_text, lang='vi')
                tts.save(test_file)
                print(f"📁 Đã tạo file test: {test_file}")
            
            # Test upload thư mục
            result = await upload_audio_directory(
                directory_path="output/audios/test",
                secret_key="your-secret-key-here"  # Thay bằng secret key thật
            )
            print("✅ Kết quả test upload thư mục:", result)
            
        except Exception as e:
            print(f"❌ Lỗi trong quá trình test: {e}")
    
    # Chạy test
    print("🚀 Test upload thư mục podcast...")
    asyncio.run(test_upload())
