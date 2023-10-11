def generate_case_combinations(word):
    if not word:
        return [""]

    first_char = word[0]
    rest_of_word = word[1:]

    rest_combinations = generate_case_combinations(rest_of_word)

    combinations = []

    for rest_combination in rest_combinations:
        combinations.append(first_char.upper() + rest_combination)
        combinations.append(first_char.lower() + rest_combination)

    return combinations
