import json
import pandas as pd
from sklearn import cross_validation
from sklearn.feature_selection import SelectPercentile, f_classif

def tokenize(speech):
    #speeches string join
    text = ' '.join(speech)
    #remove '\t'
    text = text.replace('\t', '')
    #tokenize
    from nltk.tokenize import word_tokenize
    tokens = word_tokenize(text.lower())
    #remove punctuation and stopwords
    #remove stop_words
    # import nltk
    # nltk.download('stopwords')
    from nltk.corpus import stopwords
    from string import punctuation
    stopwords_pt = set(stopwords.words('portuguese') + list(punctuation))
    tokens_w = [ch for ch in tokens if ch not in stopwords_pt]
    return tokens_w

if __name__ == '__main__':
    data = json.load(open('../data/items.jl'))

    df = pd.io.json.json_normalize(data)

    df = df.dropna(subset=['speech'])

    df['speaker'] = df.apply(lambda x: x['speaker'][0].split('-'), axis=1)

    df = df[df['speaker'].map(len) == 2]

    df['state'] = df.apply(lambda x: x['speaker'][1], axis=1)

    df['party'] = df.apply(lambda x: x['speaker'][0].split('%2C')[-1], axis=1)

    df['speaker'] = df.apply(lambda x: x['speaker'][0].split('%2C')[0], axis=1)

    df.party = pd.Categorical(df.party)

    df['target'] = df.party.cat.codes

    df['tokens'] = df.apply(lambda x: tokenize(x['speech']), axis=1)

    df['speech'] = df.apply(lambda x: ' '.join(x['speech']).replace('\t', ''), axis=1)

    features_train, features_test, labels_train, labels_test = cross_validation.train_test_split(df['speech'], df['target'], test_size=0.1, random_state=42)

    ### text vectorization--go from strings to lists of numbers
    vectorizer = TfidfVectorizer(sublinear_tf=True, max_df=0.5, stop_words=stopwords.words('portuguese'))
    features_train_transformed = vectorizer.fit_transform(features_train)
    features_test_transformed = vectorizer.transform(features_test)

    selector = SelectPercentile(f_classif, percentile=10)
    selector.fit(features_train_transformed, labels_train)
    features_train_transformed = selector.transform(features_train_transformed).toarray()
    features_test_transformed = selector.transform(features_test_transformed).toarray()
