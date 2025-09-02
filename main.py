from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
from datetime import datetime, timedelta
import requests
from dataclasses import dataclass, field, asdict
from enum import Enum
import random
from collections import defaultdict

# -----------------------
# FastAPI App
# -----------------------
app = FastAPI(title="iData API", description="Angel Broking + Analysis API")

# -----------------------
# Models
# -----------------------
# --- Input Models ---
class CandleInput(BaseModel):
    date: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int

class AnalysisRequest(BaseModel):
    candles: List[CandleInput]
    margin: float

# -----------------------
# Angel Broking / Search Models
# -----------------------
ANGEL_BASE_URL = "https://apiconnect.angelone.in/rest"

@dataclass
class Candle:
    date: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int

class Direction(str, Enum):
    none = "none"
    up = "up"
    down = "down"

@dataclass
class TimeData:
    threshold: int = 0
    time: datetime = datetime.now()
    start_value: int = 0
    end_value: int = 0
    end_time: datetime = datetime.now()
    is_enabled: bool = False
    direction: Direction = Direction.none
    cut_at: int = 0
    gain: float = 0.0
    executed_tree: bool = False

@dataclass
class DateData:
    date: datetime
    times: List[TimeData] = field(default_factory=list)

@dataclass
class Threshold:
    threshold: int = 0
    dates: List[DateData] = field(default_factory=list)

# -----------------------
# Helper Functions
# -----------------------
def average_daily_gap(candles: List[Candle]) -> float:
    daily_groups = defaultdict(list)
    for c in candles:
        day = c.date.date()
        daily_groups[day].append(c)
    gaps = [(max(day_candle.high for day_candle in v) - min(day_candle.low for day_candle in v))
            for v in daily_groups.values()]
    return sum(gaps)/len(gaps) if gaps else 0

def generate_range_values(min_val: float, max_val: float, target_count: int = 16) -> List[int]:
    start, end = int(min_val+0.5), int(max_val)
    if start > end: return []
    range_vals = []
    step = (end - start) / (target_count - 1)
    current = start
    for _ in range(target_count):
        val = int(round(current))
        if not range_vals or val != range_vals[-1]:
            range_vals.append(val)
        current += step
    range_vals[0] = start
    range_vals[-1] = end
    return range_vals

def generate_date_array(start: datetime, end: datetime) -> List[datetime]:
    dates = []
    current = start
    while current <= end:
        dates.append(current)
        current += timedelta(days=1)
    return dates

def random_in_bounds(min_val: int, max_val: int) -> int:
    if min_val == max_val: return min_val
    diff = max_val - min_val
    if diff == 1: return random.choice([min_val, max_val])
    if diff == 2: return min_val + 1
    return random.randint(min_val + 1, max_val - 1)

def get_random_hourly_candles(date: datetime, one_minute_data: List[Candle], thr: int) -> List[TimeData]:
    result = []
    hours = [9,10,11,12,13,14,15]
    for hour in hours:
        start_time = date.replace(hour=hour, minute=0, second=0)
        end_time = start_time + timedelta(hours=1)
        candles_in_hour = [c for c in one_minute_data if start_time <= c.date < end_time]
        if candles_in_hour:
            random_candle = random.choice(candles_in_hour)
            td = TimeData()
            td.time = random_candle.date
            td.start_value = random_in_bounds(int(random_candle.low), int(random_candle.high))
            td.threshold = thr
            result.append(td)
    return result

def run_thread(thr: float, time_data: TimeData, candle: Candle, margin: float) -> bool:
    cut_margin = thr * (margin / 100)
    if time_data.end_value != 0:
        return True
    is_up = candle.close > candle.open
    upper_gap = int(candle.high - candle.close)
    down_gap = int(candle.close - candle.low)
    check_high = time_data.start_value + int(thr)
    check_low = time_data.start_value - int(thr)
    candle_high = int(candle.high)
    candle_low = int(candle.low)
    if time_data.is_enabled:
        if time_data.direction == Direction.up:
            if candle_high < time_data.cut_at:
                time_data.end_value = time_data.cut_at
                time_data.end_time = candle.date
                time_data.gain = candle_high - time_data.start_value - thr
                return True
            elif candle_low < time_data.cut_at:
                time_data.end_value = time_data.cut_at
                time_data.end_time = candle.date
                time_data.gain = time_data.cut_at - time_data.start_value - thr
                return True
            else:
                if upper_gap > int(cut_margin):
                    time_data.end_value = int(candle.high - cut_margin)
                    time_data.end_time = candle.date
                    time_data.gain = candle.high - time_data.start_value - thr - cut_margin
                    return True
                if int(candle.high - cut_margin) > time_data.cut_at:
                    time_data.cut_at = int(candle.high - cut_margin)
                return False
        else:
            if candle_low > time_data.cut_at:
                time_data.end_value = time_data.cut_at
                time_data.end_time = candle.date
                time_data.gain = time_data.start_value - candle_low - thr
                return True
            elif candle_high > time_data.cut_at:
                time_data.end_value = time_data.cut_at
                time_data.end_time = candle.date
                time_data.gain = time_data.start_value - time_data.cut_at - thr
                return True
            else:
                if down_gap > int(cut_margin):
                    time_data.end_value = int(candle.low + cut_margin)
                    time_data.end_time = candle.date
                    time_data.gain = time_data.start_value - candle.low - thr - cut_margin
                    return True
                if int(candle.low + cut_margin) < time_data.cut_at:
                    time_data.cut_at = int(candle.low + cut_margin)
                return False
    else:
        if check_high <= candle_high or check_low >= candle_low:
            if upper_gap > int(thr):
                time_data.direction = Direction.up
                time_data.end_value = int(candle.high - cut_margin)
                time_data.end_time = candle.date
                time_data.gain = candle.high - time_data.start_value - thr - cut_margin
                return True
            elif down_gap > int(thr):
                time_data.direction = Direction.down
                time_data.end_value = int(candle.low + cut_margin)
                time_data.end_time = candle.date
                time_data.gain = time_data.start_value - candle.low - thr - cut_margin
                return True
            else:
                if check_high <= candle_high and check_low >= candle_low:
                    time_data.direction = Direction.up if is_up else Direction.down
                    time_data.is_enabled = True
                    time_data.cut_at = int(candle.high - cut_margin) if is_up else int(candle.low + cut_margin)
                elif check_high <= candle_high:
                    time_data.direction = Direction.up
                    time_data.is_enabled = True
                    time_data.cut_at = int(candle.high - cut_margin)
                else:
                    time_data.direction = Direction.down
                    time_data.is_enabled = True
                    time_data.cut_at = int(candle.low + cut_margin)
                return run_thread(thr, time_data, candle, margin)
        return False

def check_in_time_data(thr_obj: Threshold, time_data: TimeData, one_minute_data: List[Candle], margin: float):
    for candle in one_minute_data:
        if candle.date < time_data.time:
            continue
        if run_thread(float(thr_obj.threshold), time_data, candle, margin):
            break

def check_in_date_data(thr_obj: Threshold, date_data: DateData, one_minute_data: List[Candle], margin: float):
    for td in date_data.times:
        check_in_time_data(thr_obj, td, one_minute_data, margin)

def check_in_threshold(thr_obj: Threshold, one_minute_data: List[Candle], margin: float):
    for dd in thr_obj.dates:
        check_in_date_data(thr_obj, dd, one_minute_data, margin)

def analysis(one_minute_data: List[Candle], margin: float) -> List[Threshold]:
    thresholds: List[Threshold] = []
    average_daily = average_daily_gap(one_minute_data)
    arr_threshold = generate_range_values(average_daily / 3.0, average_daily * 1.5, 16)
    arr_threshold.sort()
    min_date = one_minute_data[0].date
    max_date = one_minute_data[-1].date
    arr_dates = generate_date_array(min_date, max_date)
    
    for t in arr_threshold:
        thr_obj = Threshold(threshold=t)
        for d in arr_dates:
            date_data = DateData(date=d)
            date_data.times = get_random_hourly_candles(d, one_minute_data, t)
            thr_obj.dates.append(date_data)
        thresholds.append(thr_obj)
    
    for thr_obj in thresholds:
        check_in_threshold(thr_obj, one_minute_data, margin)
    
    return thresholds

# -----------------------
# API Endpoints
# -----------------------
@app.post("/analyze")
def analyze_endpoint(req: AnalysisRequest):
    one_minute_data = [Candle(**c.dict()) for c in req.candles]
    result = analysis(one_minute_data, req.margin)
    return [asdict(t) for t in result]