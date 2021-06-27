import cx_Oracle
import time
db = cx_Oracle.connect('')
cursor = db.cursor()

class Dimension:
    def __init__(self, name, attribute, idd, hierarchy, map):
        self.name = name
        self.attribute = attribute
        self.id = idd
        self.hierarchy = hierarchy
        self.map = map

class Hierarchy:
    def __init__(self, name, parameter):
        self.name = name
        self.parameter = parameter


class Fact:
    def __init__(self, name, measure):
        self.name = name
        self.measure = measure

class Attribute:
    def __init__(self, name, type):
        self.name = name
        self.type = type

    def intraimputation(self):
        start = time.time()
        before2 = 0
        after2 = 0
        same = 0
        total = 0
        for d in self.dimension:
            for h in d.hierarchy:
                # imputation for parameters
                for p in h.parameter:
                    query1 = 'select count(*) from ' + d.name + " WHERE " + d.name + "." + p.name + " is null"
                    cursor.execute(query1)
                    nbatt = cursor.fetchone()
                    if nbatt[0] > 0:
                        before = nbatt[0]
                        after = 0
                        for h in d.hierarchy:
                            for p2 in h.parameter:
                                if p == p2:
                                    break
                                elif p2 != d.id:
                                    query2 = "select " + d.name + "." + p2.name + ", " + d.id.name + ' from ' + d.name + " where " + p.name + " is null and " + d.name + "." + p2name + " is not null"
                                    cursor.execute(query2)
                                    lines = cursor.fetchall()
                                    for l in lines:
                                        query3 = "select " + d.name + "."  + p.name + ' from ' + d.name + " where " + p2.name + "= '" + l[0].replace("'", "''") + "' and " + d.name + "."  + p.name + " is not null and " + d.name + "." + p2.name + " is not null"
                                        cursor.execute(query3)
                                        aimp = cursor.fetchone()
                                        if aimp:
                                            after = after +1
                                            query4 = 'update ' + d.name + " set " + p.name + "='" + aimp[0].replace("'", "''") + "' where " + d.id.name + "=" + str(l[1])
                                            cursor.execute(query4)
                                            commit = "commit"
                                            cursor.execute(commit)
                                            query5 = "select " +  p.name + ' from ' + d.name + "_ori where " + d.id.name + "=" + str(l[1])
                                            cursor.execute(query5)
                                            vt = cursor.fetchone()[0]
                                            total = total +1
                                            if vt == aimp[0]:
                                                same = same +1
                    #imputation for weak attributes
                    for key in d.map:
                        if p in d.map[key]:
                            for h in d.hierarchy:
                                if key in h.parameter:
                                    for key2 in d.map:
                                        if p in d.map[key2]:
                                            for p2 in h.parameter:
                                                if p2 != d.id:
                                                    query2 = "select " + d.name + "." + p2.name + ", " + d.id.name + ' from ' + d.name + " where " + p.name + " is null and " + d.name + "." + p2.name + " is not null"
                                                    cursor.execute(query2)
                                                    lines = cursor.fetchall()
                                                    for l in lines:
                                                        if l:
                                                            query3 = "select " + d.name + "." + p.name + ' from ' + d.name + " where " + p2.name + "= '" + \
                                                                     l[0] + "' and " + d.name + "." + p.name + " is not null"
                                                            cursor.execute(query3)
                                                            aimp = cursor.fetchone()
                                                            if aimp:
                                                                update = aimp[0].replace("'", "''")
                                                                after = after + 1
                                                                query4 = 'update ' + d.name + " set " + p.name + "='" + \
                                                                         update + "' where " + d.id.name + "=" + str(l[1])
                                                                cursor.execute(query4)
                                                                commit = "commit"
                                                                cursor.execute(commit)
                                                                query5 = "select "  + p.name + ' from ' + d.name + "_ori where " + d.id.name + "=" + str(l[1])
                                                                cursor.execute(query5)
                                                                vt = cursor.fetchone()[0]
                                                                total = total + 1
                                                                if vt == aimp[0]:
                                                                    same = same + 1
                                                if p2 == key:
                                                    break
                    before2 = before + before2
                    after2 = after2 + after
        if before2 == 0:
            return 0, 0, time.time() - start
        else:
            return after2 / before2, same/total, time.time() - start


    def interimputation(self):
        start = time.time()
        before2 = 0
        after2 = 0
        same = 0
        total = 0
        for d in self.dimension:
            for h in d.hierarchy:
                #imputation for parameters
                for p in h.parameter:
                    query1 = 'select count(*) from ' + d.name + " WHERE " + d.name + "." + p.name + " is null"
                    cursor.execute(query1)
                    nbatt = cursor.fetchone()
                    if nbatt[0] > 0:
                        before = nbatt[0]
                        after = 0
                        for d2 in self.dimension:
                            if d2 != d:
                                for h in d2.hierarchy:
                                    for p2 in h.parameter:
                                        if p == p2:
                                            break
                                        elif p2 != d2.id:
                                            query2 = "select " + d.name + "." + p2.name + ", " + d.id.name + ' from ' + d.name + " where " + p.name + " is null and " + d.name + "." + p2.name + " is not null"
                                            cursor.execute(query2)
                                            lines = cursor.fetchall()
                                            if lines:
                                                for l in lines:
                                                    if l:
                                                        query3 = "select " + d2.name + "." + a.name + ' from ' + d2.name + " where " + p.name + "= '" + \
                                                                 l[0] + "' and " + d2.name + "." + a.name + " is not null"
                                                        cursor.execute(query3)
                                                        aimp = cursor.fetchone()
                                                        if aimp:
                                                            after = after + 1
                                                            query4 = 'update ' + d.name + " set " + p.name + "='" + aimp[0] + "' where " + d.id.name + "=" + str(l[1])
                                                            cursor.execute(query4)
                                                            commit = "commit"
                                                            cursor.execute(commit)
                                                            query5 = "select " + p.name + ' from geofrance0 where ' + d.id.name + "=" + str(l[1])
                                                            cursor.execute(query5)
                                                            vt = cursor.fetchone()[0]
                                                            total = total + 1
                                                            if vt == aimp[0]:
                                                                same = same + 1
                    #imputation for weak attributes
                    for key in d.map:
                        if p in d.map[key]:
                            for d2 in self.dimension:
                                "print(d2.name)"
                                "print(type(d2.hierarchy))"
                                if d2 != d:
                                    for h in d2.hierarchy:
                                        "print(h.name)"
                                        if key in h.parameter:
                                            for key2 in d2.map:
                                                if p in d2.map[key2]:
                                                    for p2 in h.parameter:
                                                        if p2 != d2.id:
                                                            query2 = "select " + d.name + "." + p2.name + ", " + d.id.name + ' from ' + d.name + " where " + p.name + " is null and " + d.name + "." + p2.name + " is not null"
                                                            cursor.execute(query2)
                                                            lines = cursor.fetchall()
                                                            "print(lines)"
                                                            if lines:
                                                                for l in lines:
                                                                    if l:
                                                                        query3 = "select " + d2.name + "." + p.name + ' from ' + d2.name + " where " + p2.name + "= '" + \
                                                                                 l[0] + "' and " + d2.name + "." + p.name + " is not null"
                                                                        cursor.execute(query3)
                                                                        aimp = cursor.fetchone()
                                                                        if aimp:
                                                                            after = after + 1
                                                                            query4 = 'update ' + d.name + " set " + p.name + "='" + aimp[0] + "' where " + d.id.name + "=" + str(l[1])
                                                                            cursor.execute(query4)
                                                                            commit = "commit"
                                                                            cursor.execute(commit)
                                                                            query5 = "select " + p.name + ' from geofrance0 where ' + d.id.name + "=" + str(l[1])
                                                                            cursor.execute(query5)
                                                                            vt = cursor.fetchone()[0]
                                                                            total = total + 1
                                                                            if vt == aimp[0]:
                                                                                same = same + 1
                                                        if p2 == key:
                                                            break
                        before2 = before + before2
                        after2 = after2 + after
        if before2 == 0:
            return 0, 0, time.time() - start
        else:
            return after2 / before2, same / total, time.time() - start
