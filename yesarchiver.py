#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Pace Archive
#
#  Copyright 2023 Pace Software,Ltd <badelman@pacecomputer.com>
#
#
from pymongo import MongoClient
import pymongo
from threading import Thread
import time
import subprocess
from bson.objectid import ObjectId
from tkinter import Button
from tkinter import Text
from tkinter import WORD
from tkinter import Tk
from tkinter import Entry
from tkinter import Label
from tkinter import Menu
from tkinter import messagebox
from tkinter import ttk
from tkinter import filedialog
from tkcalendar import Calendar, DateEntry

import logging

top=Tk()
top.title("Yes Mongodb Archive ver 1.0")
color = 'lightgray'
top.configure(background=color)
top.config(height = 450, width = 550)
top.resizable(False, False)


logging.basicConfig(filename = 'pace_archive.log',
                    level = logging.DEBUG,
                    format = '[Update Event] %(levelname)-7.7s %(message)s')

class mongoConnect(object):
    def __init__(self):
        serv = "localhost"
        self.Client = MongoClient(serv)
        self.src_db = ""
        self.dest_db = ""
        self.dbmain = self.Client["pacemain5"]
        self.dbdemo = self.Client["demo5master"]
        self.start = time.time()
        self.threadlist = []
        self.listDatabases()
        self._load_widgets()
        self.set_buttons("disabled")
        self.set_update_buttons("disabled")
        self.mode = "convert"


    def progbar(self, name, curr, total, full_progbar):
        frac = curr/total
        filled_progbar = round(frac*full_progbar)
        print('\r', name+' '+'#'*filled_progbar + '-'*(full_progbar-filled_progbar), '[{:>7.2%}]'.format(frac), end='')

    def run_thread(self,name, func):
        Thread(target=self.run_function, args=(name, func)).start()

    def run_function(self,name, func):
        print(name, 'started')
        self.threadlist.append(name)
        func()
        print(name, 'done')
        self.threadlist.remove(name)
        self.check_threads()

    def check_threads(self):
        threads = len(self.threadlist)
        if threads == 0:
            messagebox.showinfo("Information", "All Conversion Threads are Done")
            self.post_thread_functions()
        else:
            print(str(threads), "running" , self.threadlist)


    def post_thread_functions(self):
        self.remove_orphan_accounts()
        if self.mode == "golive":
            self.golive_update_inventory()
            self.recalc_yeshead()
            messagebox.showinfo("Information", "Update Conversion Completed.")


    def calender_to_julian(self,theDate=None ):
        from datetime import timedelta, datetime
        foxStart= datetime.strptime("1980-01-01","%Y-%m-%d")
        if theDate==None:
            theday = datetime.now()
        else:
            theday = datetime.strptime(theDate,"%Y-%m-%d")
        if foxStart==theday:
            return 0
        julian = str(theday-foxStart).split(" ")[0]
        return int(julian)
    
    def set_convert_stores(self):
        self.cstores = []
        self.istores = []
        store_input = self.store_list.get()
        self.conv_stores = [item.strip() for item in store_input.split(',') if item.strip()]
        if self.conv_stores !=[]:
            for s in self.conv_stores:
                self.cstores.append({"store_id": s})
                self.istores.append({"truck_id": s})


    def get_storelist(self):
        storcur = self.src_db.yes00.find({"tag":10})
        stores = [str["store_id"] for str in storcur]
        return stores


    def listDatabases(self):
        databases = [db["name"] for db in self.Client.list_databases()]
        return databases


    def set_source_database(self,event):
        src = self.source.get()
        self.src_db = self.Client[src]
        stores = self.src_db.yes00.find({"tag": 10, "isdeleted" :0}).sort("store_id")
        store_list = ""
        for s in stores:
            store_id = s["store_id"]
            store_list +=f",{store_id}"
        # self.store_list.delete(0, top.END)
        self.store_list.insert(0, store_list) 


    def get_folder_path(self):
        extLoc = filedialog.askdirectory(parent=top, initialdir='/datastaging', title='Where should the backup be stored?')
        if messagebox.askyesno("Restore", "This will Restore all datasets in the selected Dumpfolder "+extLoc+". Continue?"):
            self.restoreDumpfolder(extLoc)

    def dump_dataset(self):
        if self.dest_db != "":
            dfolder = self.destname+"dump"
            filename = dfolder+".zip"
            bak1 = "mongodump --out completed_datasets\\"+dfolder+" --db "+self.destname
            self.subprocess_call(bak1)
            messagebox.showinfo("Info","Data Dump of "+self.destname+" is complete.")
            subprocess.Popen('explorer "completed_datasets\"')

    def restoreDumpfolder(self, folderpath):
        cmd = "mongorestore --dir "+folderpath+" --drop"
        self.subprocess_call(cmd)
        messagebox.showinfo("Info","Restore of "+folderpath+" is complete.")



    def subprocess_call(self,*args, **kwargs):
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags = subprocess.CREATE_NEW_CONSOLE | subprocess.STARTF_USESHOWWINDOW
        startupinfo.wShowWindow = subprocess.SW_HIDE
        kwargs['startupinfo'] = startupinfo
        retcode = subprocess.call(*args, **kwargs)
        return retcode
    
    def calender_to_julian(self,theDate=None ):
        from datetime import timedelta, datetime
        foxStart= datetime.strptime("1980-01-01","%Y-%m-%d")
        if theDate==None:
            theday = datetime.now()
        else:
            theday = datetime.strptime(theDate,"%Y-%m-%d")
        if foxStart==theday:
            return 0
        julian = str(theday-foxStart).split(" ")[0]
        return int(julian)
    
    def archive_data(self):
        if self.src_db != "":
            cal_date = self.cal1.get_date()
            self.conv_date = self.calender_to_julian(str(cal_date))
            print(self.conv_date)
            if messagebox.askyesno("Archive by Cutoff Date","This will delete the History based in the specfid cutoff data. Continue?"):
                invoice_list = list(self.src_db.yes01ph.find({"drawer_dt" : {"$lte": self.conv_date}}))
                for rec in invoice_list:
                    if rec["drawer_dt"] > 0:
                        self.src_db.yes01pd.delete_many({"control_no": rec["control_no"], "store_id": rec["store_id"]})
                        self.src_db.yes04d.delete_many({"control_no": rec["control_no"], "store_id": rec["store_id"]})
                        self.src_db.yes02fh.delete_many({"control_no": rec["control_no"], "store_id": rec["store_id"]})
                        self.src_db.yes02rs.delete_many({"control_no": rec["control_no"], "store_id": rec["store_id"]})



    def remove_data(self):
        if self.src_db != "":
            self.set_convert_stores()
            print(self.cstores)
            if messagebox.askyesno("Remove Store History","This will delete the history for the selected Store IDs. Continue?"):
                search_query = {"$or":self.cstores}
                self.src_db.yes01ph.delete_many(search_query)
                print("Header completed")
                self.src_db.yes01pd.delete_many(search_query)
                self.src_db.yes03e00.delete_many({"$or":self.istores})
                print("Line Items completed")
                self.src_db.yes04h.delete_many(search_query)
                print("Drawers completed")
                self.src_db.yes04d.delete_many(search_query)
                print("All completed")



    def _load_widgets(self):
        row1 = 20
        row2 = row1+30
        row3 = row2+30
        row4 = row3+30
        row5 = row4+30
        row6 = row5+30
        row7 = row6+30
        row8 = row7+30
        row9 = row8+30
        row10 = row9+30


        databases = self.listDatabases()
        try:
            menubar = Menu(top)
            filemenu = Menu(menubar, tearoff=0)
            filemenu.add_separator()
            filemenu.add_command(label="Exit", command=done)
            menubar.add_cascade(label="File", menu=filemenu)

            LSource = Label(top,text='Select DB:',bg=color,fg='black').place(x=10,y=row1)
            self.source = ttk.Combobox(top,values=databases)
            self.source.bind("<<ComboboxSelected>>", self.set_source_database)
            self.source.place(x=80,y=row1)
            self.source.insert(10,"")

            Label(top,text='Cutoff Date:',bg=color,fg='black').place(x=10,y=row2)
            self.cal1 = DateEntry(top, width= 16, background= "magenta3", foreground= "white", month = 1, day=1, year = 2015, bd=2)
            self.cal1.place(x=80,y=row2)

            btn_archive_data= Button(top,text="Archive", height=1, width=15, bg='white', command= self.archive_data)
            btn_archive_data.place(x=300,y=row2)

            lstores = Label(top,text='Specify Stores to remove All History:',bg=color,fg='black').place(x=10,y=row3)
            self.store_list = Entry(top,width=40)
            self.store_list.place(x=10,y=row4)
            self.store_list.insert(10,"")

            btn_delete_data= Button(top,text="Delete History", height=1, width=15, bg='white', command= self.remove_data)
            btn_delete_data.place(x=300,y=row4)


            btn_folder= Button(top,text="Restore Dump", height=1, width=15, bg='white', command= self.get_folder_path)
            btn_folder.place(x=80,y=row8)

            btn_dump= Button(top,text="DumpData", height=1, width=15, bg='white', command= self.dump_dataset)
            btn_dump.place(x=80,y=row9)

            btn_exit= Button(top,text="Exit", height=1, width=10, bg='white', command= done)
            btn_exit.place(x=400,y=400)

            self.buttons = []

            self.updbuttons = []

            center(top)
        except Exception as e:
            messagebox.showinfo("Error",str(e))
            logging.info("Failed to Load Widgets."+str(e)+" at %s" % time.ctime())

    def set_buttons(self,mode="normal"):
        for btn in self.buttons:
            btn["state"]= mode


    def set_update_buttons(self,mode="normal"):
        for btn in self.updbuttons:
            btn["state"]= mode


def get_ranges(divisor,totalrecs):
    retlist = []
    chunks = totalrecs/divisor
    for d in range(0,(totalrecs/chunks)+1):
        if d==0:
            retlist.append([0,chunks*d])
        else:
            retlist.append([chunks*(d-1)+1,chunks*d])
    retlist[-1][-1]=totalrecs
    return retlist

def center(toplevel):
    toplevel.update_idletasks()
    w = toplevel.winfo_screenwidth()
    h = toplevel.winfo_screenheight()
    size = tuple(int(_) for _ in toplevel.geometry().split('+')[0].split('x'))
    x = w/2 - size[0]/2
    y = h/2 - size[1]/2
    toplevel.geometry("%dx%d+%d+%d" % (size + (x, y)))

def done():
    top.quit()

class anObject(object):
    def __init__(self):
        pass

    def tellcli(self,*args):
        print(args)

def close():
    top.destroy()

mc = mongoConnect()
top.mainloop()
