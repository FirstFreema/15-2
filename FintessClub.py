import pyodbc
import json

class FitnessClubDB:
    def __init__(self, server, database, username, password):
        self.connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        self.connection = None
        self.cursor = None
        self.connect()

    def connect(self):
        try:
            self.connection = pyodbc.connect(self.connection_string)
            self.cursor = self.connection.cursor()
            print("Подключение к базе данных установлено.")
        except Exception as e:
            print("Ошибка подключения:", e)

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("Подключение к базе данных закрыто.")

    # Метод для первого запроса EXISTS
    def check_instructors_exist(self):
        query = """
        SELECT CASE WHEN EXISTS (
            SELECT 1 FROM Instructors
        ) THEN 'Существуют' ELSE 'Не существуют' END AS InstructorsExist;
        """
        # Проверка, существуют ли инструкторы в таблице
        self.cursor.execute(query)
        return self.cursor.fetchone()[0]

    # Метод для второго запроса EXISTS
    def check_visitors_in_section(self, section_id):
        query = """
        SELECT CASE WHEN EXISTS (
            SELECT 1 FROM Visits WHERE SectionID = ?
        ) THEN 'Посетители существуют' ELSE 'Посетителей нет' END AS VisitorStatus
        """
        # Проверка, есть ли посетители в указанной секции
        self.cursor.execute(query, section_id)
        return self.cursor.fetchone()[0]

    # Уникальный запрос с ANY
    def visitors_attended_earlier_than_afternoon_sessions(self):
        query = """
        SELECT FirstName, LastName
        FROM Visitors v
        WHERE EXISTS (
            SELECT 1 FROM Visits vi
            JOIN Sections s ON vi.SectionID = s.ID
            WHERE vi.VisitorID = v.ID AND s.StartTime < ANY (
                SELECT StartTime FROM Sections WHERE StartTime >= '12:00:00'
            )
        );
        """
        # Поиск посетителей, которые посещали занятия до любого занятия после 12:00
        self.cursor.execute(query)
        return self.cursor.fetchall()

    # Уникальный запрос с SOME
    def instructors_with_sessions_at_3pm(self):
        query = """
        SELECT FirstName, LastName
        FROM Instructors i
        WHERE i.SectionID = SOME (
            SELECT SectionID FROM Sections WHERE StartTime = '15:00:00'
        );
        """
        # Поиск инструкторов, которые ведут занятия в секциях, где начало в 15:00
        self.cursor.execute(query)
        return self.cursor.fetchall()

    # Метод для запроса с ALL
    def visitors_attended_all_morning_sessions(self):
        query = """
        SELECT FirstName, LastName
        FROM Visitors v
        WHERE NOT EXISTS (
            SELECT 1 FROM Sections s
            WHERE s.StartTime < '12:00:00'
            AND NOT EXISTS (
                SELECT 1 FROM Visits vi WHERE vi.VisitorID = v.ID AND vi.SectionID = s.ID
            )
        );
        """
        # Поиск посетителей, которые посещали все утренние занятия
        self.cursor.execute(query)
        return self.cursor.fetchall()

    # Метод для запроса с сочетанием ANY/SOME и ALL
    def instructors_with_all_morning_and_any_evening(self):
        query = """
        SELECT FirstName, LastName
        FROM Instructors i
        WHERE i.SectionID = ALL (
            SELECT SectionID FROM Sections WHERE StartTime < '12:00:00'
        ) AND i.SectionID = SOME (
            SELECT SectionID FROM Sections WHERE StartTime >= '17:00:00'
        );
        """
        # Поиск инструкторов, которые ведут все утренние занятия и хотя бы одно вечернее
        self.cursor.execute(query)
        return self.cursor.fetchall()

    # Метод для выполнения запроса с UNION
    def unique_names_instructors_visitors(self):
        query = """
        SELECT FirstName, LastName FROM Instructors
        UNION
        SELECT FirstName, LastName FROM Visitors;
        """
        # Получение уникальных имен из таблиц инструкторов и посетителей
        self.cursor.execute(query)
        return self.cursor.fetchall()

    # Метод для выполнения запроса с UNION ALL
    def all_names_instructors_visitors(self):
        query = """
        SELECT FirstName, LastName FROM Instructors
        UNION ALL
        SELECT FirstName, LastName FROM Visitors;
        """
        # Получение всех имен из таблиц инструкторов и посетителей, включая дубликаты
        self.cursor.execute(query)
        return self.cursor.fetchall()

    # Методы для выполнения различных JOIN-запросов
    def inner_join_visits_sections(self):
        query = """
        SELECT v.FirstName, v.LastName, s.SectionName
        FROM Visitors v
        INNER JOIN Visits vi ON v.ID = vi.VisitorID
        INNER JOIN Sections s ON vi.SectionID = s.ID;
        """
        # Получение имен и фамилий посетителей и названий секций, которые они посещали
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def left_join_visitors_sections(self):
        query = """
        SELECT v.FirstName, v.LastName, s.SectionName
        FROM Visitors v
        LEFT JOIN Visits vi ON v.ID = vi.VisitorID
        LEFT JOIN Sections s ON vi.SectionID = s.ID;
        """
        # Получение всех посетителей и соответствующих секций, включая тех, кто не посещал занятия
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def right_join_visitors_sections(self):
        query = """
        SELECT v.FirstName, v.LastName, s.SectionName
        FROM Visitors v
        RIGHT JOIN Visits vi ON v.ID = vi.VisitorID
        RIGHT JOIN Sections s ON vi.SectionID = s.ID;
        """
        # Получение секций и всех посетителей, включая секции без посетителей
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def full_join_visitors_sections(self):
        query = """
        SELECT v.FirstName, v.LastName, s.SectionName
        FROM Visitors v
        FULL JOIN Visits vi ON v.ID = vi.VisitorID
        FULL JOIN Sections s ON vi.SectionID = s.ID;
        """
        # Получение всех посетителей и всех секций, включая тех, кто не посещал занятия и секции без посетителей
        self.cursor.execute(query)
        return self.cursor.fetchall()

    # Метод для сохранения данных в JSON
    def save_to_json(self, data, filename):
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"Данные сохранены в {filename}")

# Пример использования
db = FitnessClubDB(server="localhost,1433", database="FitnessClubDB", username="SA", password="MoisseyPupkin!123;")

# Пример вызова методов и сохранения результатов
results = {
    "exists_instructors": db.check_instructors_exist(),
    "exists_visitors": db.check_visitors_in_section(1),
    "any_query": db.visitors_attended_earlier_than_afternoon_sessions(),
    "some_query": db.instructors_with_sessions_at_3pm(),
    "all_query": db.visitors_attended_all_morning_sessions(),
    "any_all_combination": db.instructors_with_all_morning_and_any_evening(),
    "union_query": db.unique_names_instructors_visitors(),
    "union_all_query": db.all_names_instructors_visitors(),
    "inner_join_query": db.inner_join_visits_sections(),
    "left_join_query": db.left_join_visitors_sections(),
    "right_join_query": db.right_join_visitors_sections(),
    "full_join_query": db.full_join_visitors_sections()
}

# Сохранение в JSON
db.save_to_json(results, "output.json")
db.close()
