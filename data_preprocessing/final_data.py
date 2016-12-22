import csv
import json
import pickle

# output file yelp_reviews.csv for recommendation
# output file business_num_name,business_num_review for LDA


business_id_set = set()
CURRENT_CITY = 'Las Vegas'.encode('utf-8')      #select city
bnum = {}       #key = business_id, value = business_number
bn = {}         #key = business_number, value = business_name
br = {}         #key = business_number, value = business_reviews

# "business_id", "full_address", "hours", "open", "categories" ,"city" ,"review_count" , "name", "neighborhoods", |
# "longitude", "state", "stars", "latitude", "attributes":, "type"
def business_extractor():
    dataset = open('yelp_academic_dataset_business.json', 'r')
    number = 1
    for line in dataset:
        line_dict = json.loads(line)
        bcity = line_dict['city'].encode('utf-8')
        if bcity != CURRENT_CITY:
            continue
        if str(line_dict['categories']).find('Restaurants') != -1:
            bname = line_dict['name'].encode('utf-8')
            bid = line_dict['business_id'].encode('utf-8')
            business_id_set.add(bid)
            bnum[bid] = number
            bn[number] = bname
            number += 1
    dataset.close()
# "votes", "user_id", "review_id", "stars", "date", "text", "type", "business_id"
def review_extractor():
    dataset = open('yelp_academic_dataset_review.json', 'r')
    writer = csv.writer(open('yelp_reviews.csv', 'w'))
    writer.writerow(['Unum', 'Bnum', 'Stars'])
    unum = {}
    number = 1
    for line in dataset:
        line_dict = json.loads(line)
        bid = line_dict['business_id'].encode('utf-8')
        if bid not in business_id_set:
            continue

        stars = line_dict['stars']
        uid = line_dict['user_id'].encode('utf-8')
        try:
            index = unum[uid]
        except KeyError:
            unum[uid] = number
            index = unum[uid]
            number += 1

        reviews = line_dict['text'].encode('utf-8').replace('\n', ' ').replace('\r', '').replace(',', ' ')
        try:
            br[bnum[bid]] += " " + reviews
        except KeyError:
            br[bnum[bid]] = "" + reviews

        writer.writerow([index, bnum[bid], stars])

    dataset.close()

# "yelping_since", "votes", "review_count", "name", "user_id", "friends", "fans", "average_stars", "type", "compliments", "elite"

business_extractor()
review_extractor()

with open('business_num_review','wb') as fp:
    print("load data...")
    pickle.dump(br,fp)

with open('business_num_name','wb') as fp:
    print("load data...")
    pickle.dump(bn,fp)