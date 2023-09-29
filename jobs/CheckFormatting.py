from _datetime import datetime


def main():
    path1 = 'abc/bac/cab/year={}/month={}/day={}'
    path2 = 'abc/bac/cab/year={}/month={}/day=2'

    current_date = datetime.now()
    print(current_date.year)
    print(current_date.month)
    print(current_date.day)

    year = current_date.year
    month = str(current_date.month).zfill(2)
    day = str(current_date.day).zfill(2)
    format_path1 = path1.format(year, month, day,'2')
    format_path2 = path2.format(year, month, day)
    print(format_path1)
    print(format_path2)



# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
