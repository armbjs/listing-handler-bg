import datetime
import pytz
import queue
import time
import pathlib
import threading
import json
import re
import os
import decimal
import logging
import math

import http.client  # ==== 알림 기능 추가 시작 ====
import urllib        # ==== 알림 기능 추가 끝 ====

import dotenv

if __package__ == None or __package__ == '':
    import redis_client
else:
    from . import redis_client

from pybit.unified_trading import HTTP

# ==== 변경 부분 시작 ====
# BG테스트 코드 사용을 위해 필요한 import
import hmac
import hashlib
import base64
import requests
from urllib.parse import urlencode
# ==== 변경 부분 끝 ====

from process_template.utils import get_number_str_with_precision

PUBSUB_CHANNEL_NAME = f"CF_NEW_NOTICES"


env_file_path = pathlib.Path(__file__).parent.parent / ".env"
print("env_file_path", env_file_path)
dotenv.load_dotenv(env_file_path, override=True)


class PubSubManager(redis_client.real_redis_client_interface.RealRedisClientInterface):
    def prepare_pubsub(self, message_handler):
        self.pubsub = self.raw_redis_client.pubsub()
        self.is_pubsub_listener_running = False
        self.is_queue_flusher_running = False
        self.message_handler = message_handler
        self.message_queue = queue.Queue()

    def subscribe(self, redis_publish_channel_key_name: str):
        self.redis_publish_channel_key_name = redis_publish_channel_key_name
        self.logger.info("Attempting to connect to Redis pubsub")
        self.start_listener_and_flusher_thread()

    def start_listener_and_flusher_thread(self):
        if self.is_pubsub_listener_running or self.is_queue_flusher_running:
            self.logger.info("이미 thread 가 실행중")
            return
        
        self.is_stopped = False

        self.logger.info(f"Subscribing to channel: {self.redis_publish_channel_key_name}")
        self.pubsub.subscribe(self.redis_publish_channel_key_name)
        self.logger.info(f"Successfully subscribed to channel: {self.redis_publish_channel_key_name}")
                
        self.pubsub_listener_thread = threading.Thread(target=self.pubsub_listener)
        self.pubsub_listener_thread.daemon = True
        self.pubsub_listener_thread.start()
        self.logger.info("Started pubsub_listener_thread thread")

        self.queue_flusher_thread = threading.Thread(target=self.queue_flusher)
        self.queue_flusher_thread.daemon = True
        self.queue_flusher_thread.start()
        self.logger.info("Started queue_flusher_thread thread")

    def queue_flusher(self):
        self.is_queue_flusher_running = True
        while True:
            try:
                if self.is_stopped:
                    break
                self.flush_message_queue()
            except Exception as e:
                self.logger.error(f"Error in queue_flusher thread: {e}", exc_info=True)
                time.sleep(1)

    def flush_message_queue(self):
        message = self.message_queue.get(block=True, timeout=None)
        self.message_handler(message)

    def insert_test_message_into_message_queue(self, data):
        self.message_queue.put(
            {
                "type": "message",
                "pattern": None,
                "channel": 'UPBIT_NEW_NOTICES',
                "data": data
            }
        )

    def pubsub_listener(self):
        self.is_pubsub_listener_running = True
        pubsub = self.pubsub
        while True:
            try:
                if self.is_stopped:
                    break

                for message in pubsub.listen():
                    if message['type'] == 'message':
                        self.message_queue.put(message)
                        self.logger.info(f"Processed pubsub message: {message['data']}")
                    elif message['type'] == 'subscribe':
                        self.logger.info("Successfully subscribed to channel")
                    else:
                        self.logger.info(f"Received message of type: {message['type']}")
                        
            except Exception as e:
                self.logger.error(f"Error in pubsub thread: {e}", exc_info=True)
                time.sleep(2)


class TradingAgent:
    def __init__(self, bybit_api_key, bybit_secret_key, telegram_redis_client, INSTANCE_NAME):
        self.bybit_api_key = bybit_api_key
        self.bybit_secret_key = bybit_secret_key
        self.telegram_redis_client = telegram_redis_client
        self.INSTANCE_NAME = INSTANCE_NAME

        # ==== 변경 부분 시작 ====
        # Bybit 관련 코드 제거, Bitget용 변수 설정
        # Bybit 클라이언트 생성 대신 BG API 키 정보 환경변수에서 읽기
        self.bg_api_key = os.environ["BG_API_KEY"]
        self.bg_secret_key = os.environ["BG_SECRET_KEY"]
        self.bg_passphrase = os.environ["BG_PASSPHRASE"]

        # BG 테스트 코드 기반 함수 사용을 위해 필요한 상수
        self.BASE_URL = "https://api.bitget.com"
        # 초기 잔고 조회를 BG로 대체
        self.spot_balance_dict = self.get_amount_dict_in_bg_spot()
        # ==== 변경 부분 끝 ====

        balance_dict = self.get_filtered_amount_dict_in_bg_spot()
        self.send_messsage_to_telegram(f"TA 시작: {balance_dict}")
        self.purchased_orders = []

    def is_order_purchased(self, order_currency, exchange):
        """주어진 order_currency와 exchange가 이미 구매되었는지 확인"""
        return (order_currency, exchange) in self.purchased_orders

    def add_purchased_order(self, order_currency, exchange):
        """주어진 order_currency와 exchange를 purchased_orders 리스트에 추가"""
        self.purchased_orders.append((order_currency, exchange))

    # ==== 알림 기능 추가 시작 ====
    def send_pushover_notification(self, title, message):
        HOST = "api.pushover.net:443"
        ENDPOINT = "/1/messages.json"
        APP_TOKEN = "abgdqz5qszbtve26nfxgdgcn2viy9z"
        USER_KEY = "u33mp5n17yesssku41o2e56cqezonq"

        params = {
            "token": APP_TOKEN,
            "user": USER_KEY,
            "message": message,
            "title": title,
            "sound": "tornado_siren",
            "priority": 1,
            "device": "bjs",
            "expire": 3600,
            "retry": 60
        }

        try:
            conn = http.client.HTTPSConnection(HOST)
            conn.request("POST", ENDPOINT, urllib.parse.urlencode(params),
                         {"Content-type": "application/x-www-form-urlencoded"})
            response = conn.getresponse()
            resp_data = response.read().decode('utf-8', errors='replace')
            if response.status == 200:
                print(f"🚨 Alert sent successfully! Response: {resp_data}")
            else:
                print(f"⚠️ Failed to send alert: {resp_data}")
        except Exception as e:
            print(f"⚠️ An error occurred while sending alert: {e}")
    # ==== 알림 기능 추가 끝 ====

    # transaction 인자를 추가하여, True일 경우 다른 Redis Stream으로 메시지를 보냄
    def send_messsage_to_telegram(self, msg, transaction=False):
        now_dt = datetime.datetime.now(tz=pytz.timezone("Asia/Seoul"))
        now_dt_str = now_dt.isoformat()
        notice_data = {
                "level": "INFO",
                "time": now_dt_str,
                "message": f"{self.INSTANCE_NAME}\n{msg}\n"
        }

        # transaction=True면 NOTICE_STREAM:RUA_UB_BN_LISTING_TRANSACTION, 아니면 기존 NOTICE_STREAM:RUA_UB_BN_LISTING
        stream_name = "NOTICE_STREAM:RUA_UB_BN_LISTING_TRANSACTION" if transaction else "NOTICE_STREAM:RUA_UB_BN_LISTING"
        self.telegram_redis_client._execute_xadd(stream_name, value_dict=notice_data)

    # ==== 변경 부분 시작 ====
    # Bybit 잔고 조회 -> BG 잔고 조회 함수로 대체
    def get_amount_dict_in_bg_spot(self):
        """Bitget 스팟 잔고 조회 후 dict로 반환"""
        resp = self.check_spot_balance()
        amount_dict = {}
        if resp.get("code") == "00000":
            for asset in resp.get("data", []):
                available = asset.get("available", "0")
                coin = asset.get("coin", "")
                if coin and float(available) > 0:
                    amount_dict[coin] = available
        return amount_dict

    def check_spot_balance(self, coin=None, assetType=None):
        endpoint = "/api/v2/spot/account/assets"
        params = {}
        if coin:
            params["coin"] = coin
        if assetType:
            params["assetType"] = assetType
        return self.send_request("GET", endpoint, params=params, need_auth=True)

    def send_request(self, method, endpoint, params=None, body=None, need_auth=False):
        if params is None:
            params = {}
        if body is None:
            body = {}

        if method.upper() == "GET" and params:
            query_string = urlencode(params)
            request_path = endpoint + "?" + query_string
            url = self.BASE_URL + request_path
            body_str = ""
        else:
            request_path = endpoint
            url = self.BASE_URL + endpoint
            body_str = json.dumps(body) if (body and method.upper() != "GET") else ""

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        if need_auth:
            ts = str(int(time.time() * 1000))
            message = ts + method.upper() + request_path + body_str
            signature = hmac.new(self.bg_secret_key.encode('utf-8'), message.encode('utf-8'), hashlib.sha256).digest()
            signature_b64 = base64.b64encode(signature).decode()
            headers["ACCESS-KEY"] = self.bg_api_key
            headers["ACCESS-SIGN"] = signature_b64
            headers["ACCESS-TIMESTAMP"] = ts
            headers["ACCESS-PASSPHRASE"] = self.bg_passphrase

        response = requests.request(method, url, headers=headers, data=body_str if method.upper() != "GET" else None)
        try:
            return response.json()
        except:
            return response.text

    def place_spot_order(self, symbol, side, orderType, force, size, price=None, clientOid=None):
        body = {
            "symbol": symbol,
            "side": side,
            "orderType": orderType,
            "force": force,
            "size": size
        }
        if price and orderType == "limit":
            body["price"] = price
        if clientOid:
            body["clientOid"] = clientOid

        endpoint = "/api/v2/spot/trade/place-order"
        result = self.send_request("POST", endpoint, body=body, need_auth=True)
        return result

    # ==== 변경 부분 끝 ====

    def update_amount_dict_in_bybit_spot(self):
        # ==== 변경 부분 시작 ====
        # bybit -> bg
        self.spot_balance_dict = self.get_amount_dict_in_bg_spot()
        # ==== 변경 부분 끝 ====

    def get_filtered_amount_dict_in_bg_spot(self):
        # 기존 함수 이름 유지, 동작은 BG 잔고 기반
        filtered_dict = {}
        for k, v in self.spot_balance_dict.items():
            if k != 'USDT':
                if v in ('0.00000000', '0.00', '0.0', '0'):
                    continue
            filtered_dict[k] = v
        return filtered_dict

    # ==== 변경 부분 시작 ====
    def buy_market_order_in_bg_spot(self, order_currency, payment_currency, value_in_payment_currency):
        # 잔고 확인 로직 전부 제거
        usdt_to_use = float(value_in_payment_currency)
        usdt_to_use = math.floor(usdt_to_use * 100) / 100.0

        # 잔고가 0 이하이거나 상관없이 바로 주문
        qty_str = f"{usdt_to_use:.2f}"
        symbol = f"{order_currency}{payment_currency}"
        order_resp = self.place_spot_order(symbol, "buy", "market", "fok", qty_str)
        return str(order_resp)

    # ==== 변경 부분 끝 ====

    def message_handler(self, message: dict):
        try:
            print(f"message_handler is called with message: {message} type(message): {type(message)}")

            notice_data_str = message['data']

            if type(notice_data_str) != str:
                print(f"notice_data_str is not a str type. notice_data_str: {notice_data_str} / type(notice_data_str): {type(notice_data_str)}")
                return

            try:
                notice_data = json.loads(notice_data_str)
            except Exception as inner_e:
                print(f"json parsing 실패 notice_data_str: {notice_data_str}")
                return

            if notice_data.get('category') == 'test':
                return

            notice_title = notice_data.get('title', '')
            if '에어드랍' in notice_title or '이벤트' in notice_title:
                return

            self.send_messsage_to_telegram(f"message: {message}")

            if notice_data['action'] != "NEW":
                return

            notice_exchange = notice_data['exchange']
            usdt_amount_in_spot_wallet = self.spot_balance_dict.get('USDT', '0') 

            if notice_exchange == 'BITHUMB':
                print("usdt_amount_in_spot_wallet : ", usdt_amount_in_spot_wallet)
                usdt_half = float(usdt_amount_in_spot_wallet) / 2
                print("usdt_half : ", usdt_half)
                usdt_amount_in_spot_wallet = get_number_str_with_precision(usdt_half, precision=4, rounding_rule=decimal.ROUND_DOWN)
                print("usdt_amount_in_spot_wallet(get_number_str_with_precision) : ", usdt_amount_in_spot_wallet)

            order_currency_list = self.extract_order_currency_list_to_buy(notice_exchange, notice_title)
            print("order_currency_list", order_currency_list)
            
            result_list = []
            filled_coins = []  # ==== 알림 기능 추가 ====

            for this_oc in order_currency_list:
                if self.is_order_purchased(this_oc, notice_exchange):
                    warning_msg = f"🔔 중복 구매 방지: {this_oc}는 이미 {notice_exchange}에서 구매되었습니다."
                    self.send_messsage_to_telegram(warning_msg, transaction=True)
                    continue  # 이미 구매한 경우 건너뜀
                try:
                    result = self.buy_market_order_in_bg_spot(this_oc, 'USDT', usdt_amount_in_spot_wallet)
                    # 체결 성공 판별 (BG API 성공 시 '00000' 코드 포함)
                    if '00000' in result:
                        filled_coins.append(this_oc)
                        self.add_purchased_order(this_oc, notice_exchange)  # 구매 성공 시 리스트에 추가
                    result_list.append(result)
                except Exception as inner_e:
                    result = f"\n\n{this_oc} exception occurred. inner_e: {inner_e} skipped...\n\n"
                    result_list.append(result)

            # 매수 성공 시 알림
            if len(filled_coins) > 0:
                filled_coins_str = ", ".join(filled_coins)
                alert_msg = f"🚨⚠️ 매수 성공 - 공지사항: {notice_title}\n매수 완료 코인: {filled_coins_str} 🚨⚠️"
                self.send_pushover_notification("매수 알림", alert_msg)

            print("result_list", result_list)
            result_str = "\n".join(result_list)
            self.send_messsage_to_telegram(result_str)

            # 여기서 체결 관련 메시지를 별도 transaction 스트림으로도 전송
            transaction_msgs = []
            for item in result_list:
                if '00000' in item:
                    # 체결 성공
                    transaction_msgs.append(f"✅ 매수 체결 데이터: {item}, purchased_order_currency: {self.purchased_orders}")
                elif "exception occurred." in item:
                    # 예외 발생 시 raw 데이터 출력
                    transaction_msgs.append(f"❌ 매수 실패(예외 발생) 데이터: {item}")
                else:
                    # '00000' 아님 = 실패. 여기서도 raw 데이터 그대로 출력
                    transaction_msgs.append(f"❌ 매수 실패 데이터: {item}")

            if transaction_msgs:
                transaction_str = "\n".join(transaction_msgs)
                self.send_messsage_to_telegram(transaction_str, transaction=True)

            self.update_amount_dict_in_bybit_spot()

        except Exception as e:
            print("message_handler exception 발생!!!", e)

    def extract_order_currency_list_to_buy(self, notice_exchange, notice_title):
        if notice_exchange == 'UPBIT':
            if ("Market Support for" in notice_title or 
                "신규 거래지원 안내" in notice_title or 
                "디지털 자산 추가" in notice_title):
                pattern = r'(\w+)\(([^)]+)\)'
                matches = re.findall(pattern, notice_title)
                crypto_names = []
                for match in matches:
                    word1, word2 = match[0], match[1]
                    if word1.isupper():
                        crypto_names.append(word1)
                    elif word2.isupper():
                        crypto_names.append(word2)
                return crypto_names

        elif notice_exchange == 'BITHUMB' and "원화 마켓 추가" in notice_title:
            pattern = r'\((\w+)\)'
            matches = re.findall(pattern, notice_title)
            return matches
        
        return []


if __name__ == '__main__':
    from pub_sub_manager import PubSubManager
    from redis_client.settings import RedisSettingsManager

    # ==== 변경 부분 시작 ====
    # BYBIT_API_KEY -> BG_API_KEY로 변경
    # BYBIT_API_SECRET -> BG_SECRET_KEY로 변경
    # INSTANCE_NAME도 변경된 대로 사용
    BG_API_KEY = os.environ["BG_API_KEY"]
    BG_SECRET_KEY = os.environ["BG_SECRET_KEY"]
    BG_PASSPHRASE = os.environ["BG_PASSPHRASE"]
    INSTANCE_NAME = os.environ["INSTANCE_NAME"]
    # ==== 변경 부분 끝 ====

    ss = {
        "service_namespace": "zoo",
        "service_name": "kabigon",
        "service_instance_id": "001",
        "service_version": "0.0.1"
    }

    env_file_path = pathlib.Path(__file__).parent.parent / ".env"
    rsm = RedisSettingsManager(env_file=env_file_path)
    rs = rsm.redis_settings_map["BJS_NOTICE_PUBSUB"]
    psm = PubSubManager(ss, rs)
    rs = rsm.redis_settings_map["RUA_COMMON_LISTING"]

    ss = {
        "service_namespace": "zoo",
        "service_name": "telegram-reporter",
        "service_instance_id": "002",
        "service_version": "0.0.1"
    }

    telegram_redis_client = PubSubManager(ss, rs)

    # ==== 변경 부분 시작 ====
    # TradingAgent 생성 시 bybit 키 대신 BG 키 사용, 그러나 구조는 유지
    # bybit_api_key, bybit_secret_key 인자는 의미 없으나 원형 유지
    ta = TradingAgent(BG_API_KEY, BG_SECRET_KEY, telegram_redis_client, INSTANCE_NAME)
    # ==== 변경 부분 끝 ====

    redis_publish_channel_key_name = PUBSUB_CHANNEL_NAME
    psm.prepare_pubsub(ta.message_handler)
    psm.subscribe(redis_publish_channel_key_name)

    # ==== 변경 부분 시작 ====
    if PUBSUB_CHANNEL_NAME != "CF_NEW_NOTICES":
        def warning_sender():
            while True:
                warning_msg = f"PUBSUB_CHANNEL_NAME가 'CF_NEW_NOTICES'가 아닌 '{PUBSUB_CHANNEL_NAME}'로 설정되어 있습니다. TEST 환경인지 확인하세요!"
                ta.send_messsage_to_telegram(warning_msg)
                time.sleep(30)

        warning_thread = threading.Thread(target=warning_sender)
        warning_thread.daemon = True
        warning_thread.start()
# ==== 변경 부분 끝 ====

    i = 0
    while True:
        if i > 0:
            if i % 300 == 0:
                ta.update_amount_dict_in_bybit_spot()  # BG 잔고 갱신
                balance_dict = ta.get_filtered_amount_dict_in_bg_spot()

            if i % 3600 == 0:            
                ta.send_messsage_to_telegram(f"현재 SPOT balance: {balance_dict}")
                ta.send_messsage_to_telegram(f"purchased_order_currency_list: {ta.purchased_orders}")

        time.sleep(1)
        i += 1
