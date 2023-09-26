import logging
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler
import pymssql
import datetime
import time
import schedule

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

token = "TOKEN_BOT_TELEGERAM"
qSrvName = "LINK_SERVER"
qDbName = "NAME_DB"
qUser = "USERNAME"
qPwd = "PASSWORD"

async def broadcast(context: ContextTypes.DEFAULT_TYPE) -> None:
    conn = pymssql.connect(server=qSrvName,user=qUser,password=qPwd,database=qDbName)
    cursor = conn.cursor()
    qStr="Select chatid from UsrteleBot where isactive = 1 "
    cursor.execute(qStr)
    resultset = cursor.fetchall()
    if resultset:
        for row in cursor:
            qStr1 = "select chat_id + ' | ' + convert(varchar,Tgl_System,11) + ' | ' + convert(varchar,jumlah_data) From SGFUAT.dbo.Monitoring"
            cursor1 = conn.cursor()
            cursor1.execute(qStr1)
            for row1 in cursor1:
                await context.bot.send_message(chat_id=row[0],text=str(row1))
            cursor1.close()
    cursor.close()
    conn.close()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = pymssql.connect(server=qSrvName,user=qUser,password=qPwd,database=qDbName)
    cursor = conn.cursor()
    qStr = "Select chatid from UsrteleBot where chatid = '" + str(update.effective_chat.id) + "' and isactive = 0 "
    cursor.execute(qStr)
    resultset = cursor.fetchall()
    if resultset:
        for row in resultset:
            current_jobs = context.job_queue.get_jobs_by_name(str(row[0]))
            for job in current_jobs:
                job.schedule_removal()
            context.job_queue.run_repeating(callback=broadcast,interval=720, first=5, chat_id=str(row[1]))
    cursor.close()
    cursor = conn.cursor()
    qStr = "Select chatid, isactive from UsrteleBot where chatid = '" + str(update.effective_chat.id) + "'"
    cursor.execute(qStr)
    resultset = cursor.fetchall()
    if not resultset:
        qStr = "Insert into UsrteleBot values('" + str(update.effective_chat.id) + "','" + str(update.effective_chat.username).replace("'","''") + "','" + str(update.effective_chat.full_name).replace("'","''") + "',0,getdate(),getdate())"
        cursor.execute(qStr)
        await context.bot.send_message(chat_id=update.effective_chat.id, text="Registering and requesting approval")
        conn.commit()
    else:
        for row in resultset:
            if row[1] == 1:
                await context.bot.send_message(chat_id=update.effective_chat.id, text="You're already registered")
            else:
                await context.bot.send_message(chat_id=update.effective_chat.id, text="Your registration is awaiting approval")
    cursor.close()
    conn.close()

async def info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = pymssql.connect(server=qSrvName,user=qUser,password=qPwd,database=qDbName)
    cursor = conn.cursor()
    qStr = "Select chatid from UsrteleBot where chatid = '" + str(update.effective_chat.id) + "' and isactive = 1 "
    cursor.execute(qStr)
    resultset = cursor.fetchall()
    if resultset:
        qStr = "select chat_id + ' | ' + convert(varchar,Tgl_system,11) + ' | ' + convert(varchar,jumlah_data) From SGFUAT.dbo.Monitoring"
        cursor.execute(qStr)
        for row in cursor: 
            await context.bot.send_message(chat_id=update.effective_chat.id,text=str(row))
    else:
        await context.bot.send_message(chat_id=update.effective_chat.id,text="You are not authorized")
    cursor.close()
    conn.close()

# async def broadcast(context: ContextTypes.DEFAULT_TYPE):
#     print("hello world")
#     conn = pymssql.connect(server=qSrvName,user=qUser,password=qPwd,database=qDbName)
#     cursor = conn.cursor()
#     qStr="Select chatid from UsrteleBot where isactive = 1 "
#     cursor.execute(qStr)
#     resultset = cursor.fetchall()
#     if resultset:
#         for row in cursor:
#             qStr1 = "select chat_id + ' | ' + convert(varchar,Tgl_System,11) + ' | ' + convert(varchar,jumlah_data) From SGFUAT.dbo.Monitoring"
#             cursor1 = conn.cursor()
#             cursor1.execute(qStr1)
#             for row1 in cursor1:
#                 await context.bot.send_message(chat_id=row[0],text=str(row1))
#             cursor1.close()
#     cursor.close()
#     conn.close()

if __name__ == '__main__':
    application = ApplicationBuilder().token(token).build()
    
    start_handler = CommandHandler('start', start)
    info_handler = CommandHandler('info',info)

    application.add_handler(start_handler)
    application.add_handler(info_handler)

    application.run_polling()

