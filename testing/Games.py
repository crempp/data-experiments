import dateparser
import re
from enum import Enum


class Region(Enum):
    Japan = 1
    NorthAmerica = 2
    PAL = 3


class ReleaseDate:
    year = None
    month = None
    day = None
    Unreleased = False
    TBA = False

    def __init__(self, date_string):
        self.parse(date_string)

    def __repr__(self):
        if self.Unreleased:
            return 'Unreleased'
        elif self.TBA:
            return 'TBA'
        else:
            s = f'{self.year}'
            if self.month is not None:
                s += f'/{self.month}'
            if self.day is not None:
                s += f'/{self.day}'
            return s

    def parse(self, date_string):
        value = None
        if date_string == 'Unreleased':
            self.Unreleased = True
        elif date_string == 'TBA':
            self.TBA = True
        elif re.search('^\d{4}$', date_string) is not None:
            self.year = int(date_string)
        else:
            try:
                date = dateparser.parse(date_string)
                self.year = date.year
                self.month = date.month
                self.day = date.day
            except AttributeError:
                parsed_date = re.findall('^(Q\d) (\d{4})$', 'Q2 2023')
                if len(parsed_date) > 0:
                    self.year = parsed_date[0][1]
                    if parsed_date[0][0] == 'Q1':
                        self.month = 3
                        self.day = 31
                    elif parsed_date[0][0] == 'Q1':
                        self.month = 6
                        self.day = 30
                    elif parsed_date[0][0] == 'Q1':
                        self.month = 9
                        self.day = 30
                    else:
                        self.month = 12
                        self.day = 31
                else:
                    raise Exception('Un-parsable Date "' + date_string + '"' )

class Developer:
    name: str = None

    def __init__(self, name: str):
        self.name = name

    def __repr__(self):
        return f'{self.name}'

class Publisher:
    name: str = None

    def __init__(self, name: str):
        self.name = name

    def __repr__(self):
        return f'{self.name}'

class PublisherList:
    publishers = []

    def upsert(self, name: str):
        publisher = list(filter(lambda d: d.name == name, self.publishers))
        if len(publisher) > 0:
            return publisher[0]
        else:
            publisher = Publisher(name)
            self.publishers.append(publisher)
            return publisher

    def __repr__(self):
        return '\n'.join([str(p) for p in self.publishers])

class DeveloperList:
    developers = []

    def upsert(self, name: str):
        developer = list(filter(lambda d: d.name == name, self.developers))
        if len(developer) > 0:
            return developer[0]
        else:
            developer = Developer(name)
            self.developers.append(developer)
            return developer

    def __repr__(self):
        return '\n'.join([str(d) for d in self.developers])

class Release:
    game = None
    region = None
    release_date = None

    def __init__(self, data):
        self.game = data['game']
        self.release_date = data['date']
        if data['region'] == 'japan':
            self.region = Region.Japan
        elif data['region'] == 'northamerica':
            self.region = Region.NorthAmerica
        else:
            self.region = Region.PAL

    def __repr__(self):
        return f'{self.game.title} {self.region} {self.release_date}'

class ReleaseList:
    releases = []

    def upsert(self, data):
        release = list(filter(lambda r: r.game.title == data['game'].title and r.region == data['region'] and r.release_date == data['date'], self.releases))
        if len(release) > 0:
            return release[0]
        else:
            release = Release(data)
            self.releases.append(release)
            return release

    def __repr__(self):
        return '\n'.join([str(r) for r in self.releases])

class Game:
    title = None
    genres = None
    developer = None
    publisher = None
    releases = []
    crossbuy = False
    crossplay = False

    def __repr__(self):
        return f"❯ {self.title} | {self.genres} | {self.developer} | {self.publisher} | {self.releases} | {self.crossbuy} | {self.crossplay}"

class GameList:
    games = []

    def upsert(self, data):
        if not self.has_game(data['title']):
            g = Game()
            self.games.append(g)
        else:
            g = [g for g in self.games if g.title == data['title']][0]
        g.title = data['title']
        g.genres =  data['genres']
        g.developer = developers.upsert(data['developer'])
        g.publisher = publishers.upsert(data['publisher'])
        g.crossbuy = data['crossbuy']
        g.crossplay = data['crossplay']
        for region, date in data['releases'].items():
            g.releases.append(releases.upsert({
                'game': g,
                'region': region,
                'date': date,
            }))


    def has_game(self, title):
        return title in [g.title for g in self.games]

    def __repr__(self):
        return "\n".join([str(g) for g in self.games])

def extract_list(bs):
  # Search for multiple items within tags
  items = bs.find_all(['a','li','p'])

  # If we didn't find anything return the plain text
  if items is None or len(items) == 0:
    return [bs.text.strip()]
  else:
    return [i.text.strip() for i in items]

games = GameList()
publishers = PublisherList()
developers = DeveloperList()
releases = ReleaseList()

for row in table.find_all('tr')[2:]:
    data = {}

    raw_cells = row.find_all(['th','td'])
    addons = extract_list(raw_cells[7])

    data['title'] = raw_cells[0].text.strip()
    data['genres'] = extract_list(raw_cells[1])
    data['developer'] = raw_cells[2].text.strip()
    data['publisher'] = raw_cells[3].text.strip()
    data['releases'] = {
        'japan': ReleaseDate(raw_cells[4].text.strip()),
        'northamerica': ReleaseDate(raw_cells[5].text.strip()),
        'pal': ReleaseDate(raw_cells[6].text.strip()),
    }
    data['crossbuy'] = 'CB' in addons
    data['crossplay'] = 'CP' in addons

    games.upsert(data)
