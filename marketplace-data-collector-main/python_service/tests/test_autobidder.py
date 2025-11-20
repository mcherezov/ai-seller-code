import unittest
from unittest.mock import Mock, patch
import pandas as pd
from datetime import datetime
import pytz

from autobidder import (
    get_settings_hash,
    apply_current_price_changes,
    validate_schedule_format,
    change_campaign_price
)


class TestAutobidder(unittest.TestCase):
    def setUp(self):
        """Подготовка тестового окружения."""
        self.moscow_tz = pytz.timezone('Europe/Moscow')
        
        # Создаем тестовые данные
        self.test_schedule = {
            "0": {"10:00": 150, "15:00": 200},  # Понедельник
            "1": {"09:00": 180, "14:00": 220},  # Вторник
            "2": {"11:00": 160, "16:00": 210},  # Среда
            "3": {"10:00": 170, "15:00": 230},  # Четверг
            "4": {"09:00": 190, "14:00": 240},  # Пятница
            "5": {"10:00": 200, "15:00": 250},  # Суббота
            "6": {"11:00": 180, "16:00": 220}   # Воскресенье
        }
        
        self.test_nm_ids = [123, 456]

        self.test_settings = pd.DataFrame([
            {
                'advert_id': 123,
                'schedule': self.test_schedule,
                'nm_ids': self.test_nm_ids,
                'legal_entity': 'inter'
            }
        ])
        
        # Мокаем функцию изменения цены
        self.price_change_mock = Mock()
        self.patcher = patch('autobidder.change_campaign_price', self.price_change_mock)
        self.patcher.start()
        
        # Мокаем функцию получения текущего времени
        self.datetime_patcher = patch('autobidder.today_msk_datetime')
        self.mock_datetime = self.datetime_patcher.start()
        
        # Мокаем загрузку конфига
        self.config_patcher = patch('autobidder.load_config')
        self.mock_config = self.config_patcher.start()
        self.mock_config.return_value = {
            'wb_autobidder_keys': {
                'inter': 'test_api_key'
            }
        }
        
    def tearDown(self):
        """Очистка после тестов."""
        self.patcher.stop()
        self.datetime_patcher.stop()
        self.config_patcher.stop()
        
    def test_get_settings_hash(self):
        """Тест создания хеша настроек."""
        hash1 = get_settings_hash(self.test_settings)
        hash2 = get_settings_hash(self.test_settings)
        
        self.assertEqual(hash1, hash2)
        self.assertNotEqual(hash1, "")
        
    def test_get_settings_hash_empty(self):
        """Тест создания хеша для пустых настроек."""
        empty_settings = pd.DataFrame()
        hash_value = get_settings_hash(empty_settings)
        self.assertEqual(hash_value, "")
        
    def test_apply_current_price_changes(self):
        """Тест применения изменений цен для текущего времени."""
        # Устанавливаем текущее время на понедельник 10:05 (после 10:00)
        current_time = datetime(2024, 1, 1, 10, 5, tzinfo=self.moscow_tz)
        self.mock_datetime.return_value = current_time
        
        # Применяем изменения
        apply_current_price_changes(self.test_settings)
        
        # Проверяем, что функция изменения цены была вызвана с правильными аргументами
        self.price_change_mock.assert_called_once_with(
            advert_id=123,
            price_change=150,
            nm_ids=self.test_nm_ids,
            legal_entity='inter'
        )
        
    def test_apply_current_price_changes_previous_day(self):
        """Тест применения цены с предыдущего дня при отсутствии прошедших изменений."""
        # Создаем расписание с будущими изменениями для понедельника
        schedule = {
            "0": {"15:00": 200, "16:00": 220},  # Только будущие изменения
            "1": {"09:00": 180, "14:00": 220},
            "2": {"11:00": 160, "16:00": 210},
            "3": {"10:00": 170, "15:00": 230},
            "4": {"09:00": 190, "14:00": 240},
            "5": {"10:00": 200, "15:00": 250},
            "6": {"11:00": 180, "16:00": 220}   # Воскресенье (предыдущий день для понедельника)
        }
        settings = pd.DataFrame([{
            'advert_id': 123,
            'schedule': schedule,
            'nm_ids': self.test_nm_ids,
            'legal_entity': 'inter'
        }])
        
        # Устанавливаем время на понедельник 12:00 (до 15:00)
        current_time = datetime(2024, 1, 1, 12, 0, tzinfo=self.moscow_tz)
        self.mock_datetime.return_value = current_time
        
        # Применяем изменения
        apply_current_price_changes(settings)
        
        # Проверяем, что была применена последняя цена с воскресенья (220)
        self.price_change_mock.assert_called_once_with(
            advert_id=123,
            price_change=220,
            nm_ids=self.test_nm_ids,
            legal_entity='inter'
        )

    def test_change_campaign_price(self):
        """Тест функции изменения цены кампании через API."""
        # Мокаем requests.patch
        with patch('requests.patch') as mock_patch:
            mock_patch.return_value.status_code = 204
            mock_patch.return_value.json.return_value = {"status": "success"}

            # Вызываем функцию
            result = change_campaign_price(123, 150, self.test_nm_ids, 'inter')

            # Проверяем, что запрос был отправлен с правильными данными
            mock_patch.assert_called_once()
            call_args = mock_patch.call_args

            # Проверяем URL (первый позиционный аргумент)
            self.assertEqual(call_args[0][0], "https://advert-api.wildberries.ru/adv/v0/bids")

            # Проверяем заголовки (второй именованный аргумент)
            self.assertEqual(call_args[1]['headers'], {
                "Authorization": "test_api_key",
                "Content-Type": "application/json"
            })

            # Проверяем payload (второй именованный аргумент)
            expected_payload = {
                "bids": [
                    {
                        "advert_id": 123,
                        "nm_bids": [
                            {"nm": 123, "bid": 150},
                            {"nm": 456, "bid": 150}
                        ]
                    }
                ]
            }
            self.assertEqual(call_args[1]['json'], expected_payload)

            # Проверяем возвращаемое значение
            self.assertIsNone(result)

    def test_change_campaign_price_error(self):
        """Тест обработки ошибки при изменении цены кампании."""
        # Мокаем requests.patch с ошибкой
        with patch('requests.patch') as mock_patch:
            mock_patch.return_value.status_code = 400
            mock_patch.return_value.text = "'advert_id'"

            # Проверяем, что функция выбрасывает исключение
            with self.assertRaises(Exception) as cm:
                change_campaign_price(123, 150, self.test_nm_ids, 'inter')

            # Проверяем текст ошибки
            self.assertIn(str(cm.exception), "Ошибка изменения цены кампании 123: 400, 'advert_id'")

    def test_apply_current_price_changes_no_schedule(self):
        """Тест ошибки при отсутствии расписания на текущий день."""
        # Создаем расписание без понедельника
        schedule = {
            "1": {"09:00": 180, "14:00": 220},
            "2": {"11:00": 160, "16:00": 210},
            "3": {"10:00": 170, "15:00": 230},
            "4": {"09:00": 190, "14:00": 240},
            "5": {"10:00": 200, "15:00": 250},
            "6": {"11:00": 180, "16:00": 220}
        }
        settings = pd.DataFrame([{
            'advert_id': 123,
            'schedule': schedule,
            'nm_ids': self.test_nm_ids,
            'legal_entity': 'inter'
        }])
        
        # Устанавливаем время на понедельник 12:00
        current_time = datetime(2024, 1, 1, 12, 0, tzinfo=self.moscow_tz)
        self.mock_datetime.return_value = current_time
        
        # Проверяем, что возникает ошибка
        with self.assertRaises(ValueError) as cm:
            apply_current_price_changes(settings)
            
        # Проверяем текст ошибки
        self.assertIn("Некорректный формат расписания", str(cm.exception))
        self.assertIn("отсутствуют некоторые дни недели", str(cm.exception))
        self.assertIn("123", str(cm.exception))
        
        # Проверяем, что функция изменения цены не была вызвана
        self.price_change_mock.assert_not_called()
        
    def test_apply_current_price_changes_empty_settings(self):
        """Тест обработки пустых настроек."""
        empty_settings = pd.DataFrame()
        
        # Проверяем, что возникает ошибка
        with self.assertRaises(ValueError) as cm:
            apply_current_price_changes(empty_settings)
            
        # Проверяем текст ошибки
        self.assertIn("Empty settings are not allowed", str(cm.exception))
        
        # Проверяем, что функция изменения цены не была вызвана
        self.price_change_mock.assert_not_called()
        
    def test_apply_current_price_changes_different_weekday(self):
        """Тест применения цены с предыдущего дня при отсутствии прошедших изменений."""
        # Создаем расписание с будущими изменениями для среды
        schedule = {
            "0": {"10:00": 150, "15:00": 200},
            "1": {"09:00": 180, "14:00": 220},  # Вторник (предыдущий день)
            "2": {"16:00": 220, "17:00": 230},  # Только будущие изменения
            "3": {"10:00": 170, "15:00": 230},
            "4": {"09:00": 190, "14:00": 240},
            "5": {"10:00": 200, "15:00": 250},
            "6": {"11:00": 180, "16:00": 220}
        }
        settings = pd.DataFrame([{
            'advert_id': 123,
            'schedule': schedule,
            'nm_ids': self.test_nm_ids,
            'legal_entity': 'inter'
        }])
        
        # Устанавливаем время на среду (день недели 2) в 15:00
        current_time = datetime(2024, 1, 3, 15, 0, tzinfo=self.moscow_tz)
        self.mock_datetime.return_value = current_time
        
        # Применяем изменения
        apply_current_price_changes(settings)
        
        # Проверяем, что была применена последняя цена со вторника (220)
        self.price_change_mock.assert_called_once_with(
            advert_id=123,
            price_change=220,
            nm_ids=self.test_nm_ids,
            legal_entity='inter'
        )
        
    def test_apply_missed_price_changes(self):
        """Тест применения пропущенных изменений цен."""
        # Устанавливаем время на понедельник 11:30 (между 10:00 и 15:00)
        current_time = datetime(2024, 1, 1, 11, 30, tzinfo=self.moscow_tz)
        self.mock_datetime.return_value = current_time
        
        # Применяем изменения
        apply_current_price_changes(self.test_settings)
        
        # Проверяем, что была применена цена из 10:00
        self.price_change_mock.assert_called_once_with(
            advert_id=123,
            price_change=150,
            nm_ids=self.test_nm_ids,
            legal_entity='inter'
        )
        
    def test_apply_missed_price_changes_multiple(self):
        """Тест применения пропущенных изменений цен с несколькими изменениями."""
        # Создаем расписание с несколькими изменениями
        schedule = {
            "0": {"10:00": 150, "12:00": 180, "15:00": 200},
            "1": {"09:00": 180, "14:00": 220},
            "2": {"11:00": 160, "16:00": 210},
            "3": {"10:00": 170, "15:00": 230},
            "4": {"09:00": 190, "14:00": 240},
            "5": {"10:00": 200, "15:00": 250},
            "6": {"11:00": 180, "16:00": 220}
        }
        settings = pd.DataFrame([{
            'advert_id': 123,
            'schedule': schedule,
            'nm_ids': self.test_nm_ids,
            'legal_entity': 'inter'
        }])
        
        # Устанавливаем время на понедельник 14:00 (после 12:00, но до 15:00)
        current_time = datetime(2024, 1, 1, 14, 0, tzinfo=self.moscow_tz)
        self.mock_datetime.return_value = current_time
        
        # Применяем изменения
        apply_current_price_changes(settings)
        
        # Проверяем, что была применена цена из 12:00
        self.price_change_mock.assert_called_once_with(
            advert_id=123,
            price_change=180,
            nm_ids=self.test_nm_ids,
            legal_entity='inter'
        )

    def test_apply_current_price_changes_single_price_change(self):
        """Тест ошибки при одном изменении цены в день."""
        # Создаем расписание с одним изменением цены
        schedule = {
            "0": {"10:00": 150},  # Только одно изменение
            "1": {"09:00": 180, "14:00": 220},
            "2": {"11:00": 160, "16:00": 210},
            "3": {"10:00": 170, "15:00": 230},
            "4": {"09:00": 190, "14:00": 240},
            "5": {"10:00": 200, "15:00": 250},
            "6": {"11:00": 180, "16:00": 220}
        }
        settings = pd.DataFrame([{
            'advert_id': 123,
            'schedule': schedule,
            'nm_ids': self.test_nm_ids,
            'legal_entity': 'inter'
        }])
        
        # Устанавливаем время на понедельник 12:00
        current_time = datetime(2024, 1, 1, 12, 0, tzinfo=self.moscow_tz)
        self.mock_datetime.return_value = current_time
        
        # Проверяем, что возникает ошибка из-за некорректного формата
        with self.assertRaises(ValueError) as cm:
            apply_current_price_changes(settings)
            
        # Проверяем текст ошибки
        self.assertIn("день 0 содержит менее 2 изменений цены", str(cm.exception))
        
        # Проверяем, что функция изменения цены не была вызвана
        self.price_change_mock.assert_not_called()
        
    def test_apply_current_price_changes_day_transition(self):
        """Тест применения цены предыдущего дня при переходе между днями."""
        # Создаем расписание с изменениями для двух дней
        schedule = {
            "0": {"10:00": 150, "15:00": 200},  # Понедельник
            "1": {"09:00": 180, "14:00": 220},  # Вторник
            "2": {"11:00": 160, "16:00": 210},
            "3": {"10:00": 170, "15:00": 230},
            "4": {"09:00": 190, "14:00": 240},
            "5": {"10:00": 200, "15:00": 250},
            "6": {"11:00": 180, "16:00": 220}
        }
        settings = pd.DataFrame([{
            'advert_id': 123,
            'schedule': schedule,
            'nm_ids': self.test_nm_ids,
            'legal_entity': 'inter'
        }])
        
        # Устанавливаем время на вторник 08:00 (после последнего изменения понедельника)
        current_time = datetime(2024, 1, 2, 8, 0, tzinfo=self.moscow_tz)
        self.mock_datetime.return_value = current_time
        
        # Применяем изменения
        apply_current_price_changes(settings)
        
        # Проверяем, что была применена цена из последнего изменения понедельника (15:00)
        self.price_change_mock.assert_called_once_with(
            advert_id=123,
            price_change=200,
            nm_ids=self.test_nm_ids,
            legal_entity='inter'
        )
        
    def test_apply_current_price_changes_all_days(self):
        """Тест проверки всех дней недели."""
        # Создаем расписание с изменениями для всех дней
        schedule = {
            "0": {"10:00": 150, "15:00": 200},  # Понедельник
            "1": {"09:00": 180, "14:00": 220},  # Вторник
            "2": {"11:00": 160, "16:00": 210},  # Среда
            "3": {"10:00": 170, "15:00": 230},  # Четверг
            "4": {"09:00": 190, "14:00": 240},  # Пятница
            "5": {"10:00": 200, "15:00": 250},  # Суббота
            "6": {"11:00": 180, "16:00": 220}   # Воскресенье
        }
        settings = pd.DataFrame([{
            'advert_id': 123,
            'schedule': schedule,
            'nm_ids': self.test_nm_ids,
            'legal_entity': 'inter'
        }])
        
        # Устанавливаем время на понедельник 12:00
        current_time = datetime(2024, 1, 1, 12, 0, tzinfo=self.moscow_tz)
        self.mock_datetime.return_value = current_time
        
        # Применяем изменения
        apply_current_price_changes(settings)
        
        # Проверяем, что была применена цена из 10:00
        self.price_change_mock.assert_called_once_with(
            advert_id=123,
            price_change=150,
            nm_ids=self.test_nm_ids,
            legal_entity='inter'
        )

    def test_validate_schedule_format(self):
        """Тест проверки формата расписания."""
        # Создаем расписание с одним изменением в день
        schedule = {
            "0": {"10:00": 150},  # Только одно изменение
            "1": {"09:00": 180, "14:00": 220},
            "2": {"11:00": 160, "16:00": 210},
            "3": {"10:00": 170, "15:00": 230},
            "4": {"09:00": 190, "14:00": 240},
            "5": {"10:00": 200, "15:00": 250},
            "6": {"11:00": 180, "16:00": 220}
        }
        
        # Проверяем, что возникает ошибка из-за некорректного формата
        with self.assertRaises(ValueError) as cm:
            validate_schedule_format(schedule, 123)
            
        # Проверяем текст ошибки
        self.assertIn("день 0 содержит менее 2 изменений цены", str(cm.exception))


if __name__ == '__main__':
    unittest.main() 