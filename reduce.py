def reduce_function(data):
    reducer_output = {}
    for line in data:
        word_key = line.split()[0]
        key_value = int(line.split()[1])
        if word_key in reducer_output.keys():
            reducer_output[word_key] += key_value
        else:
            reducer_output[word_key] = key_value
    return dict(sorted(reducer_output.items(), key=lambda x: x[1], reverse=True))