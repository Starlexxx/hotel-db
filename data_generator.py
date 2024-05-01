import hashlib
import json
from faker import Faker
from datetime import datetime, timedelta
import pymorphy3
import random


def write_to_file(data, filename):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def room_description():
    with open('room_descriptions.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
    descriptions = list(data.values())
    return random.choice(descriptions)


class DataGenerator:
    SERVICES = [
        "Ужин в номер",
        "Спа-процедуры",
        "Массаж",
        "Тренажерный зал",
        "Бассейн",
        "Сауна",
        "Экскурсии",
        "Трансфер",
        "Прокат автомобилей",
        "Прокат велосипедов",
        "Прокат лыж",
        "Прачечная",
        "Химчистка",
        "Бизнес-центр",
        "Конференц-зал",
    ]

    def __init__(self):
        self.fake = Faker('ru_RU')
        self.morph = pymorphy3.MorphAnalyzer(lang='ru')
        self.room_numbers = [i for i in range(1, 31)]
        self.client_ids = [i for i in range(1, 31)]
        self.date_rooms = {}
        self.TIME_FORMAT = "%d/%m/%Y"

    def generate_client(self):
        client_id = self.client_ids.pop()
        arrival_date = self.fake.date_between(start_date='-10y', end_date='now')
        living_days = self.fake.random_int(min=1, max=30)
        departure_date = arrival_date + timedelta(days=living_days)
        period = f"{arrival_date.strftime(self.TIME_FORMAT)}-{departure_date.strftime(self.TIME_FORMAT)}"
        room_id = random.choice(self.room_numbers)
        while self.date_rooms.get(period) and room_id in self.date_rooms[period]:
            room_id = random.choice(self.room_numbers)

        if period in self.date_rooms:
            self.date_rooms[period].append(room_id)

        return {
            "index": hashlib.md5(str(datetime.now().timestamp()).encode()).hexdigest(),
            "doc_type": "client",
            "id": client_id,
            "body": {
                "id_клиента": client_id,
                "карточка_регистрации": {
                    "Фамилия": self.fake.last_name(),
                    "Имя/Отчество": self.fake.first_name() + " " + self.fake.middle_name(),
                    "Дата рождения": str(self.fake.date_of_birth()),
                    "Пол": self.fake.random_element(elements=("Мужской", "Женский")),
                    "Место рождения": self.fake.city(),
                    "Документ, удостоверяющий личность": self.fake.random_element(
                        elements=("Паспорт", "Водительское удостоверение")),
                    "Серия/номер": self.fake.random_int(min=1000, max=9999),
                    "Дата выдачи": self.fake.date_between(start_date='-10y', end_date=arrival_date).strftime(
                        self.TIME_FORMAT),
                    "Код подразделения": str(self.fake.random_int(min=1000, max=9999)) + "-" + str(
                        self.fake.random_int(min=1000, max=9999)),
                    "Кем выдан": self.fake.company(),
                    "Адрес места жительства (регистрации)": self.fake.address(),
                },
                "дата_прибытия": arrival_date.strftime(self.TIME_FORMAT),
                "продолжительность_проживания": living_days,
                "услуга": random.sample(self.SERVICES, k=self.fake.random_int(min=1, max=len(self.SERVICES))),
                "id_номера": room_id
            }
        }

    def generate_room(self):
        return {
            "index": hashlib.md5(str(datetime.now().timestamp()).encode()).hexdigest(),
            "doc_type": "room",
            "id": self.room_numbers.pop(),
            "body": {
                "описание_номера": room_description(),
                "стоимость_день": self.fake.random_int(min=1000, max=5000)
            }
        }


if __name__ == "__main__":
    generator = DataGenerator()
    clients = [generator.generate_client() for _ in range(30)]
    rooms = [generator.generate_room() for _ in range(30)]
    write_to_file(clients, 'clients.json')
    write_to_file(rooms, 'rooms.json')
