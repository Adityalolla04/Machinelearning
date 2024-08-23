import random

# Lists of words to generate random sentences
subjects = ["I", "You", "He", "She", "They", "We", "The cat", "The dog", "The bird", "The elephant"]
verbs = ["am", "are", "is", "was", "were", "feel", "look", "seem", "appear", "become"]
objects = ["happy", "sad", "excited", "tired", "hungry", "confused", "calm", "sleepy", "angry", "curious"]

# Function to generate a random sentence
def generate_sentence():
    subject = random.choice(subjects)
    verb = random.choice(verbs)
    obj = random.choice(objects)
    return f"{subject} {verb} {obj}."

# Generate 100 meaningful sentences
sentences = [generate_sentence() for _ in range(100)]

# Join sentences into a paragraph
paragraph = " ".join(sentences)

# Save the paragraph to a text file
with open("paragraph.txt", "w") as file:
    file.write(paragraph)

print("Paragraph saved to paragraph.txt")
