import csv
import logging
import time
import subprocess


def setup_logging():
    """
    Настройка логирования в файл и консоль.
    """
    logging.basicConfig(
        filename='merge_users.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)


def interact_with_cli(csv_file_path):
    """
    Запускает CLI один раз и последовательно передаёт данные из CSV.

    :param csv_file_path: Путь к CSV-файлу с данными пользователей.
    """
    try:
        with open(csv_file_path, 'r') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=';')

            if 'fromid' not in reader.fieldnames or 'toid' not in reader.fieldnames:
                logging.error(
                    "CSV файл должен содержать заголовки 'fromid' и 'toid'.")
                print("Ошибка: Неверный формат CSV файла.")
                return

            process = subprocess.Popen(
                ['sudo', 'php', '/var/www/html/moodle/admin/tool/mergeusers/cli/climerger.php'],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )

            processed_pairs = set()  # Множество для отслеживания уже обработанных пар

            for row in reader:
                fromid = row['fromid'].strip()
                toid = row['toid'].strip()

                if not fromid.isdigit() or not toid.isdigit():
                    logging.warning(f"Пропущена строка с некорректными данными: fromid={fromid}, toid={toid}")
                    continue

                # Проверяем, не являются ли fromid и toid одинаковыми
                if fromid == toid:
                    logging.warning(f"Пропущена пара с одинаковыми ID: {fromid} -> {toid}")
                    continue

                # Проверяем, была ли уже обработана эта пара
                if (fromid, toid) in processed_pairs:
                    logging.warning(f"Пропущена пара с уже обработанными ID: {fromid} -> {toid}")
                    continue

                # Добавляем пару в множество обработанных
                processed_pairs.add((fromid, toid))

                process.stdin.write(f"{fromid}\n")
                process.stdin.flush()
                logging.info(f"Отправлен fromid: {fromid}")
                time.sleep(1)

                process.stdin.write(f"{toid}\n")
                process.stdin.flush()
                logging.info(f"Отправлен toid: {toid}")
                time.sleep(1)

                # Чтение и логирование вывода после каждого ввода
                while True:
                    output = process.stdout.readline()
                    if output == '' and process.poll() is not None:
                        break
                    if output:
                        decoded_output = output.strip()
                        if "Success" in decoded_output or "Error" in decoded_output:
                            logging.info(f"Результат слияния: {decoded_output}")
                            break

            # Завершаем CLI процесс через сигнал завершения
            process.stdin.write("-1\n")
            process.stdin.flush()
            logging.info("Отправлен сигнал завершения (-1).")
            process.stdin.close()  # Закрытие stdin потока
            process.wait(timeout=30)  # Ждём завершения CLI процесса
            logging.info("CLI процесс завершён.")

    except FileNotFoundError:
        logging.error(f"Файл не найден: {csv_file_path}")
        print("Ошибка: CSV файл не найден.")
    except subprocess.TimeoutExpired:
        logging.warning("CLI процесс не завершился за отведённое время.")
        print("Ошибка: CLI процесс завис.")
    except KeyboardInterrupt:
        logging.warning("Операция была прервана пользователем.")
        print("Операция прервана.")
    except Exception as e:
        logging.error(f"Общая ошибка: {e}")
        print("Произошла ошибка при обработке.")

if __name__ == "__main__":
    try:
        setup_logging()
        # Укажите путь к вашему CSV-файлу
        csv_file = "users_to_merge.csv"
        interact_with_cli(csv_file)
    except KeyboardInterrupt:
        logging.warning("Скрипт был прерван пользователем.")
        print("Скрипт прерван пользователем.")
