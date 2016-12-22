import csv
import json


business_id_set = set()
user_id_set = set()
user_review_rating_sum_dict = {}
user_review_count_dict = {}
CURRENT_CITY = 'Las Vegas'

# "business_id", "full_address", "hours", "open", "categories" ,"city" ,"review_count" , "name", "neighborhoods", |
# "longitude", "state", "stars", "latitude", "attributes":, "type"


def business_extractor():
    dataset = open('yelp_academic_dataset_business.json', 'r')
    writer = csv.writer(open('yelp_businesses.csv', 'w'))
    writer.writerow(['BID', 'BName', 'Average_Stars', 'Review_Count', 'Latitude', 'Longitude'])

    for line in dataset:
        line_dict = json.loads(line)
        bcity = line_dict['city'].encode('utf-8')
        if bcity != CURRENT_CITY:
            continue

        bid = line_dict['business_id'].encode('utf-8')
        business_id_set.add(bid)

        bname = line_dict['name'].encode('utf-8')
        #bstate = line_dict['state'].encode('utf-8')
        stars = line_dict['stars']
        review_count = line_dict['review_count']
        latitude = line_dict['latitude']
        longitude = line_dict['latitude']
        writer.writerow([bid, bname, stars, review_count, latitude, longitude])

    dataset.close()


# "votes", "user_id", "review_id", "stars", "date", "text", "type", "business_id"

def review_extractor():
    dataset = open('yelp_academic_dataset_review.json', 'r')
    writer = csv.writer(open('yelp_reviews.csv', 'w'))
    writer.writerow(['UID', 'BID', 'reviews', 'Stars'])

    for line in dataset:
        line_dict = json.loads(line)
        bid = line_dict['business_id'].encode('utf-8')
        if bid not in business_id_set:
            continue

        stars = line_dict['stars']
        uid = line_dict['user_id'].encode('utf-8')
        reviews = line_dict['text'].encode('utf-8').replace('\n', ' ').replace('\r', '').replace(',', ' ')
        user_id_set.add(uid)
        if uid in user_review_rating_sum_dict:
            user_review_rating_sum_dict[uid] = user_review_rating_sum_dict[uid] + stars
        else:
            user_review_rating_sum_dict[uid] = stars

        if uid in user_review_count_dict:
            user_review_count_dict[uid] += 1
        else:
            user_review_count_dict[uid] = 1

        writer.writerow([uid, bid, reviews, stars])

    dataset.close()

# "yelping_since", "votes", "review_count", "name", "user_id", "friends", "fans", "average_stars", "type", "compliments", "elite"

def user_extractor():
    dataset = open('yelp_academic_dataset_user.json', 'r')
    writer = csv.writer(open('yelp_users.csv', 'w'))
    writer.writerow(['UID', 'Name', 'Review_Count', 'Total_Stars', 'Average'])

    for line in dataset:
        line_dict = json.loads(line)
        uid = line_dict['user_id'].encode('utf-8')
        if uid not in user_id_set:
            continue

        name = line_dict['name'].encode('utf-8')
        review_count = line_dict['review_count']
        total_stars = user_review_rating_sum_dict[uid]

        if review_count != 0:
            average_stars = float(total_stars) / review_count
        else:
            average_stars = 0.0
        writer.writerow([uid, name, user_review_count_dict[uid], total_stars, average_stars])

    dataset.close()


business_extractor()
review_extractor()
user_extractor()

