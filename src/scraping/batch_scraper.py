import asyncio
import json
import logging
from pathlib import Path
from tiki_data import TikiPlaywrightScraper

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('batch_scraper.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

async def run_batch_scraping():
    """Chạy thu thập dữ liệu hàng loạt theo keywords dạng brand+type"""
    
    # Đọc file keywords
    keywords_file = Path(__file__).parent / 'search_keywork.json'
    with open(keywords_file, 'r', encoding='utf-8') as f:
        keywords_data = json.load(f)
    
    # Cấu hình cho từng nhóm
    configs = {
        'phone': {'max_products': 50, 'sleep': 5},
        'accessory': {'max_products': 30, 'sleep': 5},
        'clothing': {'max_products': 50, 'sleep': 5},
        'laptop': {'max_products': 30, 'sleep': 5}
    }
    
    all_keywords = []
    
    # Parse keywords theo cấu trúc brand+type
    for item in keywords_data:
        # Phone keywords: brand + type
        if 'phone' in item:
            brands = item['phone'].get('brands', [])
            types = item['phone'].get('types', [])
            
            # Tạo tổ hợp brand + type
            for brand in brands:
                for phone_type in types:
                    keyword = f"{brand} {phone_type}"
                    all_keywords.append({
                        'keyword': keyword,
                        'category': 'phone',
                        'max_products': configs['phone']['max_products'],
                        'sleep': configs['phone']['sleep']
                    })
        
        # Accessory keywords: chỉ có types
        if 'accessory' in item:
            types = item['accessory'].get('types', [])
            for acc_type in types:
                all_keywords.append({
                    'keyword': acc_type,
                    'category': 'accessory',
                    'max_products': configs['accessory']['max_products'],
                    'sleep': configs['accessory']['sleep']
                })
        
        # Clothing keywords: sex + type (+ material optional)
        if 'clothing' in item:
            sexes = item['clothing'].get('sex', [])
            types = item['clothing'].get('types', [])
            materials = item['clothing'].get('materials', [])
            
            # Tạo tổ hợp sex + type
            for sex in sexes:
                for cloth_type in types:
                    keyword = f"{cloth_type} {sex}"  # vd: "áo male", "quần female"
                    all_keywords.append({
                        'keyword': keyword,
                        'category': 'clothing',
                        'max_products': configs['clothing']['max_products'],
                        'sleep': configs['clothing']['sleep']
                    })
            
            # Thêm material combinations (optional)
            for material in materials:
                for cloth_type in types:
                    keyword = f"{cloth_type} {material}"  # vd: "áo cotton", "quần jeans"
                    all_keywords.append({
                        'keyword': keyword,
                        'category': 'clothing',
                        'max_products': configs['clothing']['max_products'],
                        'sleep': configs['clothing']['sleep']
                    })
        
        # Laptop keywords: brand + type
        if 'laptop' in item:
            brands = item['laptop'].get('brands', [])
            types = item['laptop'].get('types', [])
            assessories = item['laptop'].get('assessories', [])
            
            # Tạo tổ hợp brand + type
            for brand in brands:
                for laptop_type in types:
                    keyword = f"{brand} {laptop_type}"
                    all_keywords.append({
                        'keyword': keyword,
                        'category': 'laptop',
                        'max_products': configs['laptop']['max_products'],
                        'sleep': configs['laptop']['sleep']
                    })
            
            # Thêm accessories
            for accessory in assessories:
                all_keywords.append({
                    'keyword': f"laptop {accessory}",
                    'category': 'laptop',
                    'max_products': configs['laptop']['max_products'],
                    'sleep': configs['laptop']['sleep']
                })
    
    logging.info(f"🚀 Bắt đầu thu thập dữ liệu cho {len(all_keywords)} keywords")
    logging.info(f"📊 Tổng quan:")
    logging.info(f"   - Phone: {len([k for k in all_keywords if k['category'] == 'phone'])} keywords x {configs['phone']['max_products']} sản phẩm")
    logging.info(f"   - Accessory: {len([k for k in all_keywords if k['category'] == 'accessory'])} keywords x {configs['accessory']['max_products']} sản phẩm")
    logging.info(f"   - Clothing: {len([k for k in all_keywords if k['category'] == 'clothing'])} keywords x {configs['clothing']['max_products']} sản phẩm")
    logging.info(f"   - Laptop: {len([k for k in all_keywords if k['category'] == 'laptop'])} keywords x {configs['laptop']['max_products']} sản phẩm")
    
    # In ra một số ví dụ keywords để kiểm tra
    logging.info(f"\n📝 Ví dụ keywords sẽ scrape:")
    for category in ['phone', 'accessory', 'clothing', 'laptop']:
        category_keywords = [k['keyword'] for k in all_keywords if k['category'] == category][:3]
        if category_keywords:
            logging.info(f"   - {category}: {', '.join(category_keywords)}")
    
    # Chạy scraping cho từng keyword
    all_products = []
    
    for idx, kw_info in enumerate(all_keywords, 1):
        keyword = kw_info['keyword']
        category = kw_info['category']
        max_products = kw_info['max_products']
        sleep_time = kw_info['sleep']
        
        logging.info(f"\n{'='*80}")
        logging.info(f"📦 [{idx}/{len(all_keywords)}] Đang thu thập: '{keyword}' (Category: {category})")
        logging.info(f"   ├─ Số sản phẩm: {max_products}")
        logging.info(f"   └─ Sleep sau khi hoàn thành: {sleep_time}s")
        logging.info(f"{'='*80}\n")
        
        try:
            # Tạo scraper với cấu hình phù hợp
            scraper = TikiPlaywrightScraper(
                search_term=keyword,
                max_products=max_products,
                max_reviews=20,  # Giữ nguyên 20 reviews mỗi sản phẩm
                headless=True  # Chạy ẩn để nhanh hơn
            )
            
            # Chạy scraper
            products = await scraper.scrape()
            
            # Thêm metadata cho mỗi sản phẩm
            if products:
                for product in products:
                    product['search_keyword'] = keyword
                    product['search_category'] = category
                all_products.extend(products)
            
            logging.info(f"✅ Hoàn thành thu thập cho '{keyword}' - Thu được {len(products) if products else 0} sản phẩm")
            
        except Exception as e:
            logging.error(f"❌ Lỗi khi thu thập '{keyword}': {e}")
            continue
        
        # Sleep giữa các request
        if idx < len(all_keywords):
            logging.info(f"⏳ Đang chờ {sleep_time} giây trước khi thu thập keyword tiếp theo...")
    
    logging.info(f"\n{'='*80}")
    logging.info(f"🎉 HOÀN THÀNH! Đã thu thập xong {len(all_keywords)} keywords")
    logging.info(f"📊 Tổng số sản phẩm: {len(all_products)}")
    logging.info(f"{'='*80}")
    
    return all_products

if __name__ == "__main__":
    asyncio.run(run_batch_scraping())
