import random, json


def generate_dummy_data():
    towns = ['Coleraine', 'Banbridge', 'Belfast', 'Lisburn', 'LondonDerry', 'Newry', 'Enniskillen', 'Omagh',
             'Ballymena']
    businesses_list = []

    for i in range(100):
        name = "biz " + str(i)
        town = towns[random.randint(0, len(towns) - 1)]
        rating = random.randint(1, 5)
        businesses_list.append({
            "name": name,
            "town": town,
            "rating": rating,
            "reviews": []
        })
    return businesses_list


businesses = generate_dummy_data()
fout = open("data.json", "w")
fout.write(json.dumps(businesses))
fout.close()