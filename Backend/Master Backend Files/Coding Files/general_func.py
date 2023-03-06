class GeneralFunc:
    def removepunc(filter_data):
        print("===filter_data input===")
        print(filter_data)
        filter_data = [i.split(",") for i in filter_data]
        filter_data = [item.strip() for sublist in filter_data for item in sublist]
        filter_data = list( dict.fromkeys(filter_data))
        filter_data = ', '.join([str(i.strip()).strip("-") for i in filter_data if i != "-" and i != " -"]).title()
        filter_data = filter_data.strip()
        return filter_data