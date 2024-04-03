from profanity_check import predict

_author_ = 'vbs'


def count_swear_words(text):
    # Get the profanity predictions for each word in the text
    predictions = predict(text.split())
    # Count the number of predicted profanity words
    count = sum(predictions)

    return count


lyrics = "Your goddamit this shit, lyrics go here with potential swear words."
num_swear_words = count_swear_words(lyrics)

print(f"Number of swear words: {num_swear_words}")
