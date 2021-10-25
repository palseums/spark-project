list1 = [1,2,3,4]
for i in list1:
    if (i % 2) == 0:
        print(i, "even")
    else:
        print(i, "odd")

new1 = ["even" if (i % 2) == 0 else "odd" for i in list1]
print(new1)

