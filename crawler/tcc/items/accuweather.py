# -*- coding: utf-8 -*-
import logging

from scrapy import Field

from gaivota.dbhelper import DBHelper
from gaivota.items.baseitem import BaseItem


class AccuweatherBaseItem(BaseItem):
    gaivota_odp_id = Field()
    location_key = Field()

    @staticmethod
    def presave(db, spider_id, items):
        logger = logging.getLogger('presave_method')

        # Find all unique ids in all items
        location_keys = set([item['location_key'] for item in items])

        # Search equivalent odp_id for each unique id
        id_odp_map = DBHelper.get_odp_id(db, spider_id, 'custom_id_str', location_keys)

        ready_items = []
        for item in items:
            if item['location_key'] not in id_odp_map:
                msg = "Couldn't find location_key={} in the database"
                logger.error(msg.format(item['location_key']))
                continue

            item['gaivota_odp_id'] = id_odp_map[item['location_key']]
            ready_items.append(item)

        return ready_items


class AccuweatherNowItem(AccuweatherBaseItem):
    date = Field()
    time = Field()
    temperature = Field()
    apparent_temperature = Field()
    sky_status = Field()
    wind_direction = Field()
    wind_speed = Field()
    uv_index = Field()
    relative_humidity = Field()
    msl_pressure = Field()
    cloud_ceiling = Field()
    cloud_cover = Field()
    dewpoint_temperature = Field()
    horizontal_visibility = Field()

    __tablename__ = 'accu_now'
    __sqlcolumns__ = [
        'gaivota_odp_id', 'date', 'time', 'temperature', 'apparent_temperature',
        'sky_status', 'wind_direction', 'wind_speed', 'relative_humidity',
        'msl_pressure', 'uv_index', 'cloud_cover', 'cloud_ceiling',
        'dewpoint_temperature', 'horizontal_visibility'
    ]

class AccuweatherHourlyForecastItem(AccuweatherBaseItem):
    forecast_date = Field()
    forecast_time = Field()
    created_at = Field()
    temperature = Field()
    apparent_temperature = Field()
    sky_status = Field()
    wind_direction = Field()
    wind_speed = Field()
    uv_index = Field()
    relative_humidity = Field()
    cloud_cover = Field()
    dewpoint_temperature = Field()
    horizontal_visibility = Field()
    probability_of_rain = Field()
    # probability_of_snow = Field()
    # probability_of_ice = Field()

    __tablename__ = 'accu_hourly_forecast'
    __sqlcolumns__ = [
        'gaivota_odp_id', 'created_at', 'forecast_date', 'forecast_time',
        'temperature', 'apparent_temperature', 'sky_status', 'wind_direction',
        'wind_speed', 'relative_humidity', 'uv_index', 'cloud_cover',
        'dewpoint_temperature', 'horizontal_visibility', 'probability_of_rain'
    ]

class AccuweatherHistoricalItem(AccuweatherBaseItem):
    date = Field()
    hi_temperature = Field()
    lo_temperature = Field()
    precipitation = Field()
    historical_avg_hi = Field()
    historical_avg_lo = Field()

    __tablename__ = 'accu_historical'
    __sqlcolumns__ = [
        'gaivota_odp_id', 'date', 'hi_temperature', 'lo_temperature',
        'precipitation', 'historical_avg_hi', 'historical_avg_lo'
    ]

class AccuweatherDailyForecastItem(AccuweatherBaseItem):
    created_at = Field()
    forecast_date = Field()
    is_day = Field()
    hilo_temperature = Field()
    hilo_apparent_temperature = Field()
    sky_status = Field()
    wind_direction = Field()
    wind_speed = Field()
    wind_gusts = Field()
    high_uv_index = Field()
    precipitation = Field()
    hours_of_precipitation = Field()
    probability_of_rain = Field()
    probability_of_thunderstorms = Field()

    __tablename__ = 'accu_daily_forecast'
    __sqlcolumns__ = [
        'gaivota_odp_id', 'created_at', 'forecast_date', 'is_day',
        'hilo_temperature', 'hilo_apparent_temperature', 'probability_of_rain',
        'sky_status', 'wind_direction', 'wind_speed', 'wind_gusts',
        'probability_of_thunderstorms', 'high_uv_index',
        'precipitation', 'hours_of_precipitation'
    ]

class AccuweatherAstronomyItem(AccuweatherBaseItem):

    date = Field()
    sunset = Field()
    sunrise = Field()
    moonset = Field()
    moonrise = Field()

    __tablename__ = 'accu_astronomy'
    __sqlcolumns__ = [
        'gaivota_odp_id', 'date', 'sunset', 'sunrise', 'moonset', 'moonrise'
    ]
