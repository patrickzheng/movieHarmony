
# coding: utf-8

# In[2]:

from nltk.corpus import wordnet as wn


# In[3]:

print wn.synsets('laugh')
print
print wn.synsets('cry')
print 
print wn.synsets('love')
print
print wn.synsets('fight')
print 
print wn.synsets('think')


# In[4]:

from nltk.corpus import wordnet as wn
import re
moodList = wn.synsets('action')
finalMoodList = [list(mood.closure(lambda s: s.hyponyms())) for mood in moodList]
finalMoodList = [item.lemma_names() for moodList in finalMoodList for item in moodList]
finalMoodList = [item for moodList in finalMoodList for item in moodList]
pattern = re.compile(r'[A-Z]|\_|\-')
finalMoodList = [elem for elem in finalMoodList if not pattern.findall(elem)]
#finalMoodList = [list(mood.closure(lambda s: s.hypernyms())) for mood in moodList]
finalMoodList = set(finalMoodList)
finalMoodList = sorted(finalMoodList)
finalMoodList
#hypo = lambda s: s.hyponyms()
#hyper = lambda s: s.hypernyms()
#list(dog.closure(hypo, depth=1)) == dog.hyponyms()
#list(dog.closure(hyper, depth=1)) == dog.hypernyms()
#list(dog.closure(hypo))


# In[5]:

#wn.morphy('plays')


# In[6]:

testStr = "I bought this for my husband who plays the piano.  He is having a wonderful time playing these old hymns.  The music  is at times hard to read because we think the book was published for singing from more than playing from.  Great purchase though!"


# In[7]:

from collections import Counter
import nltk
from nltk.tokenize import TweetTokenizer
tknzr = TweetTokenizer(strip_handles=True, reduce_len=True)
cnt = Counter()
for word in tknzr.tokenize(testStr):
    word = wn.morphy(word)
    if word in finalMoodList: cnt[word] += 1
    #cnt[word] += 1
print cnt
print sum(cnt.values())


# In[8]:

#reviewerSummaryRDD1 = rating.map(lambda row: (row.asin, row.reviewText))


# In[ ]:



