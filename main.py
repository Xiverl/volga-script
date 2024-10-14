import os
from datetime import datetime
from urllib.parse import urlencode

import asyncio
import aiohttp
import pandas as pd
from sqlalchemy import (
    create_engine, Column, Integer, Float, String, DateTime
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import aioconsole

# Константы
SKOLTECH_LATITUDE = 55.69782222
SKOLTECH_LONGITUDE = 37.36156389


# Настройка базы данных
Base = declarative_base()
engine = create_engine("sqlite:///weather_data.db")
Session = sessionmaker(bind=engine)


class WeatherData(Base):
    """Модель данных погоды."""

    __tablename__ = "weather_data"
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    temperature = Column(Float)
    wind_speed = Column(Float)
    wind_direction = Column(String)
    pressure = Column(Float)
    precipitation_type = Column(String)
    precipitation_amount = Column(Float)


Base.metadata.create_all(engine)


async def get_weather_data():
    """Функция для получения данных о погоде."""

    async with aiohttp.ClientSession() as session:
        base_url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": SKOLTECH_LATITUDE,
            "longitude": SKOLTECH_LONGITUDE,
            "current_weather": True,
            "hourly": (
                "temperature_2m,relativehumidity_2m,windspeed_10m,"
                "winddirection_10m,pressure_msl,precipitation"
            )
        }
        url = f"{base_url}?{urlencode(params)}"

        async with session.get(url) as response:
            data = await response.json()
            current = data["current_weather"]
            hourly = data["hourly"]

            # Преобразование направления ветра в буквенное обозначение
            wind_direction_map = ["С", "СВ", "В", "ЮВ", "Ю", "ЮЗ", "З", "СЗ"]
            wind_direction = wind_direction_map[
                int((current["winddirection"] + 22.5) % 360) // 45
            ]

            return {
                "temperature": current["temperature"],
                "wind_speed": current["windspeed"],
                "wind_direction": wind_direction,
                # Конвертация из гПа в мм рт.ст.
                "pressure": hourly["pressure_msl"][0] * 0.75006,
                "precipitation_type": (
                    "rain" if hourly["precipitation"][0] > 0 else "none"
                ),
                "precipitation_amount": hourly["precipitation"][0]
            }


async def save_to_db(data):
    """Функция для сохранения данных в БД."""

    session = Session()
    weather_data = WeatherData(**data)
    session.add(weather_data)
    session.commit()
    session.close()


async def export_to_excel():
    """Функция для экспорта данных в Excel."""

    session = Session()
    data = session.query(WeatherData).order_by(
        WeatherData.timestamp.desc()
    ).limit(10).all()
    session.close()

    df = pd.DataFrame([
        {
            "Timestamp": d.timestamp,
            "Temperature (°C)": d.temperature,
            "Wind Speed (m/s)": d.wind_speed,
            "Wind Direction": d.wind_direction,
            "Pressure (mmHg)": d.pressure,
            "Precipitation Type": d.precipitation_type,
            "Precipitation Amount (mm)": d.precipitation_amount
        } for d in data
    ])

    filename = f"weather_data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx"
    df.to_excel(filename, index=False)
    print(f"Данные экспортированы в файл: {filename}")


async def print_instructions():
    """Функция для вывода инструкций пользователю."""

    print("\n" + "=" * 50)
    print("Скрипт сбора данных о погоде запущен и работает.")
    print("=" * 50)
    print("\nИнструкция по использованию:")
    print("1. Скрипт автоматически собирает данные о погоде каждые 3 минуты.")
    print(
        "2. Для экспорта последних 10 записей в Excel файл, введите \"export\"."
    )
    print("3. Для выхода из программы введите \"exit\".")
    print("\nОжидание ввода команды...")


async def weather_loop():
    """Основная функция для запроса данных о погоде."""

    while True:
        try:
            weather_data = await get_weather_data()
            await save_to_db(weather_data)
            print(
                "\n"
                f"Данные о погоде сохранены: "
                f"{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}"
            )
        except Exception as e:
            print(f"Произошла ошибка: {e}")
        await asyncio.sleep(180)  # Ждем 3 минуты


async def handle_user_input():
    """Функция для обработки команд пользователя."""

    await print_instructions()
    while True:
        command = await aioconsole.ainput("Введите команду (export/exit): ")
        if command.lower() == "export":
            await export_to_excel()
        elif command.lower() == "exit":
            print("Завершение работы...")
            os._exit(0)
        else:
            print("Неизвестная команда. Введите \"export\" или \"exit\".")


async def main():
    """Основная функция."""

    print("Запуск скрипта сбора данных о погоде...")
    await asyncio.gather(
        weather_loop(),
        handle_user_input()
    )

if __name__ == "__main__":
    asyncio.run(main())
