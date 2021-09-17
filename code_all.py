#Imports
import numpy as np
import pandas as pd
import random
import time
import cx_Oracle
from itertools import chain
import matplotlib.pyplot as plt
import itertools
import random

###Random Initialization
def create_DB_conn():
    dsn_tns = cx_Oracle.makedsn('localhost', '1521', service_name='xe') # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
    conn = cx_Oracle.connect(user=r'FTA_USER', password='password', dsn=dsn_tns) # if needed, place an 'r' before any parameter in order to address special characters such as '\'. For example, if your user name contains '\', you'll need to place 'r' before the user name: user=r'User Name'
    return conn

def oned_to_twod(li,x,y):
    new_li = []
    for i in range(0,x):
        column = []
        for j in range(0,y):
            column.append(li[(i*y+j)])
        new_li.append(column)
    #print(new_li)
    return new_li

def create_2d(x,y):
    new_li = []
    for i in range(0,x):
        column = []
        for j in range(0,y):
            column.append([])
        new_li.append(column)
    return new_li

def get_day_emp_list(li):
    new_li = []
    
    for i in li:
        try:
            for j in i:
                new_li.append(j)
        except:
            return '(NULL)'
    if not new_li:
        return '(NULL)'
    else:
        return str(new_li).replace('[','(').replace(']',')')

def create_req_list(conn):
    c = conn.cursor()
    d = conn.cursor()
    c.execute('select skill,min(employee_id),max(employee_id) from employee_tb group by skill order by skill') #use triple quotes if you want to spread your query across multiple lines
    d.execute('select * from requirements_tb')
    listi = []
    for row in c:
        listi.append([row[1],row[2]])
    clean_emp = listi[0]
    counter_emp = listi[1]
    kitchen_emp = listi[2]
    #print(clean_emp,counter_emp,kitchen_emp)
    li = []
    ola = []
    for row in d:
        ola.append(row)
        try:
            li.append([int(row[3]),int(row[4]),int(row[5])])
        except:
            #print(row)
            None #Ritam
    #print(li)
    new_li = oned_to_twod(li,7,6) 
    #print(ola[0])
    #print(new_li[0][0])
    return new_li
    conn.close()
    
def find_violation_shifts(violation_matrix):
    violation_li = []
    for day in range(0,7):
        for shift in range(0,6):
            if violation_matrix[day][shift]:
                violation_li.append((day,shift))
    return violation_li

def outsourcing_Cost(sol):
    flattened_sol = flatten(sol)
    return flattened_sol.count(0)   
    
    
def get_prior_violation(conn,sol):
    #prior violations are fetched
    d = conn.cursor()
    tot = 0
    flattened_sol = flatten(sol)
    flattened_sol = set(flattened_sol)
    flattened_sol = str(flattened_sol).replace('{','(').replace('}',')')
    sql_statement1 = 'select sum(violations) from fairness_bank_tb where  employee_id in '+ flattened_sol
    #print(sql_statement1)
    d.execute(sql_statement1)
    for row in d:
        tot = tot + row[0] 
    return tot
    
def soft_constraint1_violation(conn,sol,week):
    #Here we focus on personal leave violations
    #dsn_tns = cx_Oracle.makedsn('localhost', '1521', service_name='xe') # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
    #conn = cx_Oracle.connect(user=r'FTA_USER', password='password', dsn=dsn_tns) # if needed, place an 'r' before any parameter in order to address special characters such as '\'. For example, if your user name contains '\', you'll need to place 'r' before the user name: user=r'User Name'
    d = conn.cursor()
    tot = 0
    violation_li = []
    violation_matrix = create_2d(7,6)
    #print("oh no")
    for day in range(0,7):
        for shift in range(0,6):
            if sol[day][shift]:
                #print('week day')
                #print(week)
                #print(day)
                #print(sol[day][shift])
                #print(get_day_emp_list(sol[day][shift]))
                sql_statement1 = 'select count(*) from emp_preference_tb where preference_id = \'P_1\'  AND  to_date(VALUE,\'dd-mm-yyyy\')  - to_date(\'01-07-2021\',\'dd-mm-yyyy\') = '+str(week)+' * 7 + '+str(day) + ' and  employee_id in '+ get_day_emp_list(sol[day][shift])
                #print(sql_statement1) #Ritam
                d.execute(sql_statement1)
                for row in d:
                    tot = tot + row[0]
                sql_statement2 = 'select employee_id from emp_preference_tb where preference_id = \'P_1\'  AND to_date(VALUE,\'dd-mm-yyyy\')  - to_date(\'01-07-2021\',\'dd-mm-yyyy\') = '+str(week)+' * 7 + '+str(day) + ' and  employee_id in '+ get_day_emp_list(sol[day][shift])
                #print('check')
                #print(sql_statement2)
                d.execute(sql_statement2)
                for row in d:
                        violation_li.append((row[0],day,shift))
                        violation_matrix[day][shift].append(row[0])
                if len(sol[day][shift])>1:
                    if 0 in sol[day][shift][0]:
                        violation_matrix[day][shift].append(0)
                    elif 0 in sol[day][shift][1]:
                        violation_matrix[day][shift].append(0)
                    elif 0 in sol[day][shift][2]:
                        violation_matrix[day][shift].append(0)
    return tot,violation_matrix

def soft_constraint2_violation(conn,sol):
    #Here we focus on violations of weekend holiday requirement
    #dsn_tns = cx_Oracle.makedsn('localhost', '1521', service_name='xe') # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
    #conn = cx_Oracle.connect(user=r'FTA_USER', password='password', dsn=dsn_tns) # if needed, place an 'r' before any parameter in order to address special characters such as '\'. For example, if your user name contains '\', you'll need to place 'r' before the user name: user=r'User Name'
    d = conn.cursor()
    tot = 0
    violation_li = []
    violation_matrix = create_2d(7,6)
    for day in range(5,7):
        for shift in range(0,6):
            if sol[day][shift]:
                sql_statement1 = 'select count(*) from emp_preference_tb where preference_id = \'P_3\'    '+ ' and  employee_id in '+ get_day_emp_list(sol[day][shift])
                #print(sql_statement1)
                d.execute(sql_statement1)
                for row in d:
                    tot = tot + row[0]
                sql_statement2 = 'select employee_id from emp_preference_tb where preference_id = \'P_3\'    '+ ' and  employee_id in '+ get_day_emp_list(sol[day][shift])
                d.execute(sql_statement2)
                for row in d:
                    violation_li.append((row[0],day,shift))
                    violation_matrix[day][shift].append(row[0])
    return tot,violation_matrix

#FOr utilizing fairness bank
def cost(conn,sol,week,update_fairness_bank=False):
    soft1 = soft_constraint1_violation(conn,sol,week)
    soft2 = soft_constraint2_violation(conn,sol)
    cost = soft1[0] + soft2[0] + get_prior_violation(conn,sol) + outsourcing_Cost(sol)
    
    #cost = sum( weight * soft constraints violated)
    #weighted cost to be included -> sum(1/priority)
    violation_li = soft1[1] + soft2[1]
    if update_fairness_bank:
        flatten_list = list(chain.from_iterable(soft1[1]))
        dsn_tns = cx_Oracle.makedsn('localhost', '1521', service_name='xe') # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
        conn = cx_Oracle.connect(user=r'FTA_USER', password='password', dsn=dsn_tns) # if needed, place an 'r' before any parameter in order to address special characters such as '\'. For example, if your user name contains '\', you'll need to place 'r' before the user name: user=r'User Name'
        sql_statement1 =  'update fairness_bank_tb set VIOLATIONS = VIOLATIONS + '+ str(1) +' where EMPLOYEE_ID IN ' + get_day_emp_list(flatten_list);
        #print(sql_statement1)
        d = conn.cursor()
        d.execute(sql_statement1)
        conn.commit()
        conn.close() 
    return cost,violation_li


def create_changed_solution(conn,sol,week):
    violation_matrix = cost(conn,sol,week)[1]
    #Find someone to swap
    violated_shifts = find_violation_shifts(violation_matrix)
    if violated_shifts:
        random_shift = random.sample(find_violation_shifts(violation_matrix),1)
        #Find someone to swap with
        swap_emp = violation_matrix[random_shift[0][0]][random_shift[0][1]]
    
        replace_who = random.sample(swap_emp,1)[0]
        d = conn.cursor()
        sql_statement = 'SELECT employee_id FROM ( SELECT employee_id FROM emp_preference_tb where employee_id not in '+'('+','.join("%d" % i for i in swap_emp)+')' + 'ORDER BY dbms_random.value) WHERE rownum = 1'
        d.execute(sql_statement)
        for row in d:
            replace_with = row[0]
        ls = list(map(lambda x: replace_with if x==replace_who else x, swap_emp))
        sol[random_shift[0][0]][random_shift[0][1]] = ls
    if not violated_shifts:
        None
    return sol

def create_random_solution(conn):
    c = conn.cursor()
    d = conn.cursor()
    c.execute('select skill,min(employee_id),max(employee_id) from employee_tb group by skill order by skill') #use triple quotes if you want to spread your query across multiple lines
    d.execute('select * from requirements_tb')
    listi = []
    for row in c:
        listi.append([row[1],row[2]])
    clean_emp = listi[0]
    counter_emp = listi[1]
    kitchen_emp = listi[2]
    #print(clean_emp,counter_emp,kitchen_emp)
    li = []
    ola = []
    for row in d:
        ola.append(row)
        try:
            li.append([random.sample([j for j in range(clean_emp[0],clean_emp[1])],int(row[3])),random.sample([j for j in range(counter_emp[0],counter_emp[1])],int(row[4])),random.sample([j for j in range(kitchen_emp[0],kitchen_emp[1])],int(row[5]))])
        except:
            #print(row)
            None #Ritam
    #print(li)
    new_li = oned_to_twod(li,7,6) 
    #print(ola[0])
    #print(new_li[0][0])
    return new_li
    conn.close()

def soft_constr_chk(conn,emp_id,day,shift,week):
    violation_count = 0
    d = conn.cursor()
    sql_statement1 = 'select count(*) from emp_preference_tb where preference_id = \'P_1\'  AND  to_date(VALUE,\'dd-mm-yyyy\') - to_date(\'01-07-2021\',\'dd-mm-yyyy\') = '+str(week)+' * 7 + '+str(day) + ' and  employee_id = '+ str(emp_id)
    if day >= 5:
        sql_statement2 = 'select count(*) from emp_preference_tb where preference_id = \'P_3\'    '+ ' and   employee_id = '+ str(emp_id)
        d.execute(sql_statement2)
        for row in d:
            violation_count = violation_count + row[0]
    d.execute(sql_statement1)
    for row in d:
        violation_count = violation_count + row[0]
    if violation_count>0:
        return True
    else:
        return False
  
 
def hard_constr_chk(conn,emp_id,shifts):
    sql_statement1 = 'select value from emp_preference_tb where preference_id = \'P_4\' and  employee_id = '+ str(emp_id)
    d = conn.cursor()
    d.execute(sql_statement1)
    #print(sql_statement1)
    for row in d:
        if int(row[0]) <shifts:
            return True
    return False  


def fill_employee(emp_id):
    shift_list = create_2d(7,6)
    for day in range(0,7):
        for shift in range(0,6):
            shift_list[day][shift] = emp_id
    return shift_list


def create_greedy_solution(conn):
    c = conn.cursor()
    c.execute('select sum(kitchen),sum(counter),sum(cleaning) from requirements_tb')
    skill_emp_all = [[],[],[]]
    for row in c:
        #print(row)
        shift_skill_req = list(row)
    shift_skill_emp=[]
    c.execute('select skill,count(*) from employee_tb group by skill order by 1 desc')
    for row in c:
        shift_skill_emp.append(row[1])
    div_shifts  = []
    for i,j in zip(shift_skill_req,shift_skill_emp):
        div_shifts.append(i//j)

    c.execute('select * from employee_tb where skill = \'Kitchen\'')
    for row in c:
        skill_emp_all[0].append(row[0])
    c.execute('select * from employee_tb where skill = \'Counter\'')
    for row in c:
        skill_emp_all[1].append(row[0])
    c.execute('select * from employee_tb where skill = \'Cleaning\'')
    for row in c:
        skill_emp_all[2].append(row[0])

    random.shuffle(skill_emp_all[0])
    random.shuffle(skill_emp_all[1])
    random.shuffle(skill_emp_all[2])

    sol_list = create_2d(7,6)
    week=1
    #fill skill 1

    req_list = create_req_list(create_DB_conn())
    for day in range(0,7):
            for shift in range(0,6):
                sol_list[day][shift] = [[],[],[]]

    for i in range(0,3):
        for emp_id in skill_emp_all[i]:
            shifts_assn = 0
            for day in range(0,7):
                for shift in range(0,6):
                    if len(sol_list[day][shift][i]) <= req_list[day][shift][i] and shifts_assn < div_shifts[i]:
                        if not soft_constr_chk(conn,emp_id,day,shift,week) and not hard_constr_chk(conn,emp_id,shifts_assn):
                            sol_list[day][shift][i].append(emp_id)
                            shifts_assn = shifts_assn + 1


    for i in range(0,3):
        for day in range(0,7):
                for shift in range(0,6):
                    diff = req_list[day][shift][i] - len(sol_list[day][shift][i])
                    for j in range(0,diff):
                        sol_list[day][shift][i].append(0)


    return sol_list

    
def flatten(sol):
    merged = list(itertools.chain(*sol))
    merged = list(itertools.chain(*merged))
    not_integers = [x for x in merged if not isinstance(x, int)]
    not_integers = list(itertools.chain(*not_integers))
    integers = [x for x in merged if isinstance(x, int)]
    return not_integers+integers

def isHardConstraintViolated(sol,conn):
    flattened_sol = flatten(sol)
    count_shifts = {x:flattened_sol.count(x) for x in flattened_sol}
    sql_statement1 = 'select employee_id,value,priority from emp_preference_tb where preference_id = \'P_4\''
    d = conn.cursor()
    d.execute(sql_statement1)
    for row in d:
        if row[0] in count_shifts:
            if int(count_shifts[row[0]]) < int(row[1]):
                return False
    return True 

def iterated_local_search_basic(max_iter = 50, max_iter_without_improvement = 10, show_graph = False, show_time = False,week = 1):
    start = time.time()
    conn = create_DB_conn()
    global_minima_cost = 10000
    cost_li = []
    initial_sol = create_random_solution(conn)
    global_minima_sol = initial_sol
    for i in range(0,max_iter): 
        local_minima_sol = create_random_solution(conn)
        attempt_num = 0
        while (attempt_num <= max_iter_without_improvement): #loop 2 (while max_iter_without_improvement has not been crossed):
            changed_sol = create_changed_solution(conn,local_minima_sol,week)
            if  isHardConstraintViolated(changed_sol,conn) and cost(conn,changed_sol,3) < cost(conn,local_minima_sol,3):
                #instead of two checks include hard and soft constraints in cost
                local_minima_sol = changed_sol
            else:
                attempt_num = attempt_num + 1
            cost_li.append(cost(conn,local_minima_sol,3)[0])
        if cost(conn,local_minima_sol,3) < cost(conn,global_minima_sol,3):
            global_minima_sol = local_minima_sol
    final_cost = cost(conn,global_minima_sol,3,update_fairness_bank=True)
    conn.commit()
    conn.close() 
    #print(cost_li)
    if show_graph:
        plt.plot( [i for i in range(0,len(cost_li))],cost_li)
        plt.xlabel('Iteration')
        plt.ylabel('Soft constraint violations')
        plt.show()
    end = time.time()
    return final_cost[0],global_minima_sol

def iterated_local_search_greedy(max_iter = 50, max_iter_without_improvement = 10, show_graph = False, show_time = False,week = 1):
    start = time.time()
    conn = create_DB_conn()
    global_minima_cost = 10000
    cost_li = []
    initial_sol = create_greedy_solution(conn)
    global_minima_sol = initial_sol
    for i in range(0,max_iter): 
        local_minima_sol = create_greedy_solution(conn)
        attempt_num = 0
        while (attempt_num <= max_iter_without_improvement): #loop 2 (while max_iter_without_improvement has not been crossed):
            changed_sol = create_changed_solution(conn,local_minima_sol,week)
            if  isHardConstraintViolated(changed_sol,conn) and cost(conn,changed_sol,3) < cost(conn,local_minima_sol,3):
                #instead of two checks include hard and soft constraints in cost
                local_minima_sol = changed_sol
            else:
                attempt_num = attempt_num + 1
            cost_li.append(cost(conn,local_minima_sol,3)[0])
        if cost(conn,local_minima_sol,3) < cost(conn,global_minima_sol,3):
            global_minima_sol = local_minima_sol
    final_cost = cost(conn,global_minima_sol,3,update_fairness_bank=True)
    conn.commit()
    conn.close() 
    #print(cost_li)
    if show_graph:
        plt.plot( [i for i in range(0,len(cost_li))],cost_li)
        plt.xlabel('Iteration')
        plt.ylabel('Soft constraint violations')
        plt.show()
    end = time.time()
    if show_time:
        print(end - start)
    return final_cost[0],global_minima_sol
    
def preference_query(emp_id,conn):
    d = conn.cursor()
    sql_statement1 =  'update emp_preference_tb set TEMP_HARD_CONSTRAINT = \'Y\' where EMPLOYEE_ID = ' + str(emp_id);
    d.execute(sql_statement1)
    conn.commit()
    cost = iterated_local_search(10,5)
    sql_statement2 =  'update emp_preference_tb set TEMP_HARD_CONSTRAINT = NULL where EMPLOYEE_ID = ' + str(emp_id);
    d.execute(sql_statement2)
    conn.commit()
    print('Your preferences could not be satisfied since ' +str(cost) + ' preferences of other employees were being violated')
    print('We will try to fulfil your requirements next week.')
