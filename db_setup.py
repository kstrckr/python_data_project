import sqlite3

def db_connect(db_name):
        con = sqlite3.connect(db_name)
        return con

class StoreSchema:

    store_table_cols = '''
        store_number INTEGER PRIMARY KEY, 
        store_name TEXT NOT NULL, 
        address TEXT, 
        city TEXT,
        zip_code INTEGER, 
        store_lat REAL, 
        store_long REAL, 
        county_id INTEGER NOT NULL, 
        FOREIGN KEY (county_id) REFERENCES counties(county_id)'''
            
    table_statement = 'CREATE TABLE IF NOT EXISTS stores'

    def create_stores_table(self):
        full_statement = self.table_statement + '(' + self.store_table_cols + ')'
        return full_statement

class CountySchema:

    counties = [
        (1, 'Adair'),
        (2, 'Adams'),
        (3, 'Allamakee'),
        (4, 'Appanoose'),
        (5, 'Audubon'),
        (6, 'Benton'),
        (7, 'Black Hawk'),
        (8, 'Boone'),
        (9, 'Bremer'),
        (10, 'Buchanan'),
        (11, 'Buena Vista'),
        (12, 'Butler'),
        (13, 'Calhoun'),
        (14, 'Carroll'),
        (15, 'Cass'),
        (16, 'Cedar'),
        (17, 'Cerro Gordo'),
        (18, 'Cherokee'),
        (19, 'Chickasaw'),
        (20, 'Clarke'),
        (21, 'Clay'),
        (22, 'Clayton'),
        (23, 'Clinton'),
        (24, 'Crawford'),
        (25, 'Dallas'),
        (26, 'Davis'),
        (27, 'Decatur'),
        (28, 'Delaware'),
        (29, 'Des Moines'),
        (30, 'Dickinson'),
        (31, 'Dubuque'),
        (32, 'Emmet'),
        (33, 'Fayette'),
        (34, 'Floyd'),
        (35, 'Franklin'),
        (36, 'Fremont'),
        (37, 'Greene'),
        (38, 'Grundy'),
        (39, 'Guthrie'),
        (40, 'Hamilton'),
        (41, 'Hancock'),
        (42, 'Hardin'),
        (43, 'Harrison'),
        (44, 'Henry'),
        (45, 'Howard'),
        (46, 'Humboldt'),
        (47, 'Ida'),
        (48, 'Iowa'),
        (49, 'Jackson'),
        (50, 'Jasper'),
        (51, 'Jefferson'),
        (52, 'Johnson'),
        (53, 'Jones'),
        (54, 'Keokuk'),
        (55, 'Kossuth'),
        (56, 'Lee'),
        (57, 'Linn'),
        (58, 'Louisa'),
        (59, 'Lucas'),
        (60, 'Lyon'),
        (61, 'Madison'),
        (62, 'Mahaska'),
        (63, 'Marion'),
        (64, 'Marshall'),
        (65, 'Mills'),
        (66, 'Mitchell'),
        (67, 'Monona'),
        (68, 'Monroe'),
        (69, 'Montgomery'),
        (70, 'Muscatine'),
        (71, 'O\'brien'),
        (72, 'Osceola'),
        (73, 'Page'),
        (74, 'Palo Alto'),
        (75, 'Plymouth'),
        (76, 'Pocahontas'),
        (77, 'Polk'),
        (78, 'Pottawattamie'),
        (79, 'Poweshiek'),
        (80, 'Ringgold'),
        (81, 'Sac'),
        (82, 'Scott'),
        (83, 'Shelby'),
        (84, 'Sioux'),
        (85, 'Story'),
        (86, 'Tama'),
        (87, 'Taylor'),
        (88, 'Union'),
        (89, 'Van Buren'),
        (90, 'Wapello'),
        (91, 'Warren'),
        (92, 'Washington'),
        (93, 'Wayne'),
        (94, 'Webster'),
        (95, 'Winnebago'),
        (96, 'Winneshiek'),
        (97, 'Woodbury'),
        (98, 'Worth'),
        (99, 'Wright')
    ]

    county_table_cols = '''
        county_id INTEGER PRIMARY KEY,
        county_name TEXT NOT NULL UNIQUE
    '''

    table_statement = 'CREATE TABLE IF NOT EXISTS counties'

    def create_counties_table(self):
        full_statement = self.table_statement + '(' + self.county_table_cols + ')'
        return full_statement

    def insert_counties(self, database):

        insert_statement = '''INSERT INTO counties(county_id, county_name)
                                VALUES(?,?)'''
        
        for county in self.counties:
            database.execute(insert_statement, county)

if __name__ == "__main__":
    New_ILS_DB = db_connect('sales.db')
    store_schema = StoreSchema()
    New_ILS_DB.execute(store_schema.create_stores_table())