titles = {}
books = File.readlines("/Users/avi/Downloads/bx/BX-Books.csv")
books.shift
books.each do |line|
  begin
    isbn, title, *rest = line.split(";")
    titles[isbn] = eval(title).split("(")[0].downcase
  rescue SyntaxError
  end
end

ratings = File.readlines("/Users/avi/Downloads/bx/BX-Book-Ratings.csv")
ratings.shift
ratings.each do |line|
  user, isbn, rating = line.split(";")
  if t = titles[isbn]
    puts [t, eval(user)].join("\t")
  end
end

