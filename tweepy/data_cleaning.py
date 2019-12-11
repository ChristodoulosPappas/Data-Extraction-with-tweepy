import re
import emoji
import demoji

##Using regular expressions to extract http/https links and emojis in order
##to clean our data

demoji.download_codes();
def extract_emojis(str):
  return ''.join(c for c in str if c in emoji.UNICODE_EMOJI)

text = "@RMonnar @myWitsEnnd Bankruptcy breaks farmers hearts. Losing the family farm is truly heartbreaking. 💔💔😢😢"

emojis = extract_emojis(text);
em = demoji.findall(emojis);

print(len(emojis));
print(em)
for i in range(len(emojis)):
	text = re.sub(emojis[i],' ',text);

text = re.sub('RT',' ',text);

x1 = re.findall(r'@[A-Za-z0-9-_:]+',text);
print(x1);
text  = re.sub(r'@[A-Za-z0-9-_:]+',"",text)

x2 = re.findall('https?://[A-Za-z0-9./]+',text);
print(x2)
text = re.sub('https?://[A-Za-z0-9./]+',"",text);
print(text)
