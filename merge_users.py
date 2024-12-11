import csv
import pty
import os
import logging
import re
import signal
import time

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
                logging.error("CSV файл должен содержать заголовки 'fromid' и 'toid'.")
                print("Ошибка: Неверный формат CSV файла.")
                return

            master, slave = pty.openpty()  # Открываем псевдотерминал
            process = os.fork()

            if process == 0:  # Дочерний процесс
                os.dup2(slave, 0)  # stdin
                os.dup2(slave, 1)  # stdout
                os.dup2(slave, 2)  # stderr
                os.close(master)
                os.execvp('sudo', ['sudo', 'php', '/var/www/html/moodle/admin/tool/mergeusers/cli/climerger.php'])
            else:  # Родительский процесс
                os.close(slave)

                with os.fdopen(master, 'r+b', buffering=0) as fd:
                    for row in reader:
                        fromid = row['fromid'].strip()
                        toid = row['toid'].strip()

                        if not fromid.isdigit() or not toid.isdigit():
                            logging.warning(f"Пропущена строка с некорректными данными: fromid={fromid}, toid={toid}")
                            continue

                        # Передача fromid
                        fd.write(f"{fromid}\n".encode())
                        fd.flush()

                        # Передача toid
                        fd.write(f"{toid}\n".encode())
                        fd.flush()

                        output = b""
                        while True:
                            data = fd.read(1024)
                            output += data
                            decoded_output = output.decode(errors='ignore')
                            if re.search(r'\d+: Success; Log id: \d+', decoded_output):  # Успешное завершение
                                break
                            if "ошибка" in decoded_output.lower():  # Возможная ошибка
                                break

                        success_message = extract_success_message(output.decode(errors='ignore'))
                        if success_message:
                            logging.info(success_message)
                            print(success_message)
                        else:
                            logging.error(f"Ошибка слияния: fromid={fromid}, toid={toid}. Вывод: {output.decode(errors='ignore')}")
                            print(f"Ошибка: fromid={fromid}, toid={toid}")

                    # Завершаем CLI процесс через Ctrl-C
                    os.kill(process, signal.SIGINT)
                    time.sleep(1)  # Даем процессу завершиться

    except FileNotFoundError:
        logging.error(f"Файл не найден: {csv_file_path}")
        print("Ошибка: CSV файл не найден.")
    except KeyboardInterrupt:
        logging.warning("Операция была прервана пользователем.")
        print("Операция прервана.")
    except Exception as e:
        logging.error(f"Общая ошибка: {e}")
        print("Произошла ошибка при обработке.")

def extract_success_message(output):
    """
    Извлекает сообщение об успешном объединении из вывода CLI.

    :param output: Строка вывода CLI.
    :return: Сообщение об успехе или None.
    """
    match = re.search(r"From \d+ to \d+: Success; Log id: \d+", output)
    return match.group(0) if match else None

if __name__ == "__main__":
    try:
        setup_logging()
        # Укажите путь к вашему CSV-файлу
        csv_file = "users_to_merge.csv"
        interact_with_cli(csv_file)
    except KeyboardInterrupt:
        logging.warning("Скрипт был прерван пользователем.")
        print("Скрипт прерван пользователем.")