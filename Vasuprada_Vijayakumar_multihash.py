import sys
import itertools

def bitmapCheck1(bitmap, itemset,bucket_size):
    sum = 0
    for item in itemset:
        sum += ord(item)
    hashvalue = sum % bucket_size
    if (1 << hashvalue) & bitmap != 0:
        return True
    else:
        return False

def bitmapCheck2(bitmap, itemset,bucket_size):
    prime = 13
    sum = prime
    for item in itemset:
        sum += ord(item)
    hashvalue = sum % bucket_size
    if (1 << hashvalue) & bitmap != 0:
        return True
    else:
        return False

def count_itemsets(old_items,bitvector1,bitvector2,baskets,itemset_size,support,bucket_size):
    result = []
    comb_list = [list(item) for item in old_items]
    Candidate_frequent = {}
    for items in baskets:
        for subset in set(itertools.combinations(items, itemset_size)):
            check_counter = 0
            temp_subset = itertools.combinations(subset, itemset_size - 1)
            new_temp_list = [list(item) for item in temp_subset]
            for i in new_temp_list:
                if i in comb_list:  # Condition 1 :to check if in (i,j) i and j are both frequent
                    check_counter += 1
            # Condition 2 and Condition 3 - to check if hashed to frequent bucket in both hashtables
            if bitmapCheck1(bitvector1,list(subset),bucket_size) and bitmapCheck2(bitvector2,list(subset),bucket_size) and check_counter == len(new_temp_list):
                if subset in Candidate_frequent.keys():
                    Candidate_frequent[subset] += 1
                else:
                    Candidate_frequent[subset] = 1

    for k,v in Candidate_frequent.iteritems():
        if v >= support:
            result.append(list(k))
    return sorted(result)


def createbitmap(hashtable,support):
    bitmap = 0
    for k,v in hashtable.iteritems():
        if v >= support:
            bitmap += 1 << k
    return bitmap

def constructHashTable2(baskets,itemset_size,bucket_size):
    # Hash Function = i + j + 13 mod 10
    hashtable2 = {}
    prime = 13
    for items in baskets:
        subsets = []
        for subset in set(itertools.combinations(items, itemset_size)):
            subsets.append(subset)
        for itemset in subsets:
            sum = prime
            for item in itemset:
                sum += ord(item)
            hashKey = sum % bucket_size
            if hashKey in hashtable2.keys():
                hashtable2[hashKey] += 1
            else:
                hashtable2[hashKey] = 1
    return hashtable2

def constructHashTable1(baskets,itemset_size,bucket_size):
    # Hash Function = i + j mod 10
    hashtable1 = {}
    for items in baskets:
        subsets = []
        for subset in set(itertools.combinations(items, itemset_size)): # FIND SUBSETS OF A LIST GIVEN SIZE
            subsets.append(subset)
        for itemset in subsets:
            sum = 0
            for item in itemset:
                sum += ord(item)
            hashKey = sum % bucket_size
            if hashKey in hashtable1.keys():
                hashtable1[hashKey] += 1
            else:
                hashtable1[hashKey]  = 1

    return hashtable1

def frequent_singles(baskets,support):
    singles = {}
    frequent_items = []
    for items in baskets:
        for item in items:
            if item in singles.keys():
                singles[item] += 1
            else:
                singles[item] = 1

    for k, v in singles.iteritems():
        if v >= support:
            frequent_items.append(k)

    return sorted(frequent_items)

def main(argv):

    baskets = []
    support = int(argv[2])
    bucket_size = int(argv[3]) # USED '10' AS THE BUCKET SIZE FOR EACH HASH FUNCTION

    with open (argv[1]) as f:
        content = f.readlines()
        for line in content:
            items = line.strip().split(",")
            baskets.append(sorted(items))

        # To generate the initial frequent singles
        frequent_itemsets = frequent_singles(baskets, support)
        itemset_size = 1
        while len(frequent_itemsets) > 0:
            if itemset_size == 1:
                frequent_itemsets = frequent_singles(baskets,support)
                print frequent_itemsets,"\n"
            else:
                bitvector1 = createbitmap(hashtable_1,support)
                bitvector2 = createbitmap(hashtable_2,support)
                old_items = frequent_itemsets
                frequent_itemsets = count_itemsets(old_items,bitvector1,bitvector2,baskets,itemset_size,support,bucket_size)
                print frequent_itemsets,"\n"

            itemset_size += 1
            hashtable_1 = constructHashTable1(baskets, itemset_size, bucket_size)
            hashtable_2 = constructHashTable2(baskets, itemset_size, bucket_size)

            if len(frequent_itemsets) == 0:
                sys.exit()

            print hashtable_1
            print hashtable_2


if __name__ == "__main__":
    main(sys.argv)