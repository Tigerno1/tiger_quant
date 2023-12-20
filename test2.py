has_vegetarian = input("Is anyone in your part a vegetarian (yes/no) ? ").strip().lower()
has_vegan = input("Is anyone in your part a vegan (yes/no) ? ").strip().lower()
has_gluten_free = input("Is anyone in your part gluten-free (yes/no) ? ").strip().lower()

print("Here are your restaurant choices:")

if has_vegetarian in ("yes", "no") and has_vegan in ("yes", "no") and has_gluten_free in ("yes", "no"):
    print("The Chef's Kitchen")
    print("Corner Cafe")
    
    if has_vegetarian == "yes"and has_vegan == "no":
        print("Main Street Pizza")
        if has_gluten_free == "no":
            print("Mama's Fine Italian")
        
    elif has_vegetarian == "no" and has_vegan == "no" and has_gluten_free == "no":
            print("Joe's Gourmet Burgers")
            
else:
    print("illegal input; your input should be either 'yes' or 'no'!")