import sys
import itertools
import random
frequent_itemsets_dic = {}


def count_items_sample(previousFrequentitems,basket_aftersample,sample_size,support):
    frequent_itemsets_sample = {}
    negative_border = {}
    candidate_sample = {}
    for basket in basket_aftersample:
        for subset in set(itertools.combinations(basket, sample_size)):
            check_counter = 0
            temp_subset = itertools.combinations(subset, sample_size - 1)
            make_list = [list(item) for item in temp_subset]
            for item in make_list:
                if item in previousFrequentitems:
                    check_counter += 1
            if check_counter == len(make_list):
                if subset in candidate_sample.keys():
                    candidate_sample[subset] += 1
                else:
                    candidate_sample[subset] = 1
    flag_item_found = 0
    for k, v in candidate_sample.iteritems():
        if v >= support:
            frequent_itemsets_sample[k] = v
            flag_item_found = 1
        else:
            negative_border[k] = v

    return flag_item_found,frequent_itemsets_sample,negative_border




def count_itemsets(old_items,itemset_size,baskets,support):
    candidate_frequent = {}
    for basket in baskets:
        for subset in set(itertools.combinations(basket, itemset_size)):
            check_counter = 0
            temp_subset = itertools.combinations(subset, itemset_size - 1)
            make_list = [list(item) for item in temp_subset]
            for item in make_list:
                if item in old_items:
                    check_counter += 1
            if check_counter == len(make_list):
                if subset in candidate_frequent.keys():
                    candidate_frequent[subset] += 1
                else:
                    candidate_frequent[subset] = 1
    flag_item_found = 0
    for k,v in candidate_frequent.iteritems():
        if v >= support:
            frequent_itemsets_dic[k] = v
            flag_item_found = 1

    return flag_item_found,frequent_itemsets_dic

def frequent_sample_singles(baskets,support):
    frequent_itemsets_sample = {}
    negative_border = {}
    singles = {}
    for items in baskets:
        for item in items:
            if item in singles.keys():
                singles[item] += 1
            else:
                singles[item] = 1

    for k, v in singles.iteritems():
        if v >= support:
            frequent_itemsets_sample[k] = v
        else:
            negative_border[k] = v

    return frequent_itemsets_sample,negative_border



def frequent_singles(baskets,support):
    singles = {}
    for items in baskets:
        for item in items:
            if item in singles.keys():
                singles[item] += 1
            else:
                singles[item] = 1

    for k, v in singles.iteritems():
        if v >= support:
            frequent_itemsets_dic[k] = v

    return frequent_itemsets_dic


def main(argv):

    baskets = []
    fraction = 0.5  # 50% sample set size
    support = int(argv[2])
    bucket_size = 10
    adjusted_support = 0.9 * fraction * support

    f = open(argv[1])
    content = f.readlines()
    for line in content:
        items = line.strip().split(",")
        baskets.append(sorted(items))

    size = len(baskets)


    #basket_sample = random.sample(baskets,int(fraction*size))
    frequent_itemsets = frequent_singles(baskets, support)
    itemset_size = 2
    while 1:
        old_items = [list(k) for k in frequent_itemsets.keys()]
        flag,frequent_itemsets = count_itemsets(old_items,itemset_size,baskets,support)
        if flag == 0:
            break
        itemset_size += 1

    #print "Whole File:",frequent_itemsets
    dataset_frequent_list = [k for k in frequent_itemsets.keys()]
    #print "Whole Frequent:",dataset_frequent_list

    check_sample = True
    no_of_iterations = 1

    while check_sample:

        basket_aftersample = random.sample(baskets,int(fraction*size))
        #print "Sample Chosen:", basket_aftersample
        #print
        candidate_frequent, negative_border = frequent_sample_singles(basket_aftersample, int(adjusted_support))
        candidate_frequent_dic = candidate_frequent.copy()
        #print "Candidate_Single",candidate_frequent_dic
        negative_border_dic = negative_border.copy()
        #print "NegativeBorder_Single",negative_border_dic
        #print

        sample_size = 2

        while 1:
             previousFrequent = [list(k) for k in candidate_frequent_dic.keys()]
             flag,candidate_frequent,negative_border = count_items_sample(previousFrequent,basket_aftersample,sample_size,int(adjusted_support))
             candidate_frequent_dic.update(candidate_frequent)
             negative_border_dic.update(negative_border)
             if flag == 0:
                break
             sample_size += 1

        sample_frequent_list = [k for k in candidate_frequent_dic.keys()]
        #print "Frequent -Sample:", sample_frequent_list


        sample_negative_border = [k for k in negative_border_dic.keys()]
        #print "Negative Border-Sample:", sample_negative_border
        #print


        # FALSE POSITIVE

        if len(list(set(sample_frequent_list) & set(dataset_frequent_list))) == len(dataset_frequent_list):
            break

        # FALSE NEGATIVE

        if len(list(set(sample_negative_border) & set(dataset_frequent_list))) != 0:
            check_sample = True
            negative_border_dic = {}
            candidate_frequent_dic = {}
        no_of_iterations += 1


    print no_of_iterations
    print fraction
    result = [list(x) for x in list(set(sample_frequent_list) & set(dataset_frequent_list))]
    final_itemsets = []

    for x in range(len(max(result, key=len))):
        temp = []
        for true_itemset in result:
            if len(true_itemset) == x + 1:
                temp.append(true_itemset)
        final_itemsets.append(temp)

    for ans in final_itemsets:
        print sorted(ans)
        print




if __name__ == "__main__":
    main(sys.argv)