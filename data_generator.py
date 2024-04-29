import json
from faker import Faker
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
        self.room_numbers = [i for i in range(1, 101)]

    def generate_sentence(self):
        sentence = self.fake.sentence()
        words = sentence.split(' ')
        for i in range(len(words)):
            p = self.morph.parse(words[i])[0]
            inflected_word = p.inflect({'nomn'})
            words[i] = inflected_word.word if inflected_word else words[i]
        return ' '.join(words)

    def generate_client(self):
        arrival_date = str(self.fake.date_between(start_date='-10y', end_date='now'))
        return {
            "index": self.fake.random_int(min=1, max=100),
            "doc_type": "client",
            "id": self.fake.uuid4(),
            "body": {
                "id_клиента": self.fake.random_int(min=1, max=100),
                "карточка_регистрации": {
                    "Фамилия": self.fake.last_name(),
                    "Имя/Отчество": self.fake.first_name() + " " + self.fake.middle_name(),
                    "Дата рождения": str(self.fake.date_of_birth()),
                    "Пол": self.fake.random_element(elements=("Мужской", "Женский")),
                    "Место рождения": self.fake.city(),
                    "Документ, удостоверяющий личность": self.fake.random_element(
                        elements=("Паспорт", "Водительское удостоверение")),
                    "Серия/номер": self.fake.random_int(min=1000, max=9999),
                    "Дата выдачи": arrival_date,
                    "Код подразделения": str(self.fake.random_int(min=1000, max=9999)) + "-" + str(
                        self.fake.random_int(min=1000, max=9999)),
                    "Кем выдан": self.fake.company(),
                    "Адрес места жительства (регистрации)": self.fake.address(),
                },
                "дата_прибытия": arrival_date,
                "продолжительность_проживания": self.fake.random_int(min=1, max=30),
                "услуга": random.sample(self.SERVICES, k=self.fake.random_int(min=1, max=len(self.SERVICES))),
                "id_номера": random.choice(self.room_numbers)
            }
        }

    def generate_room(self):
        return {
            "index": self.fake.random_int(min=1, max=100),
            "doc_type": "room",
            "id": random.choice(self.room_numbers),
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
