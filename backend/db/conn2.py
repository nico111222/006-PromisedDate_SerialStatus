import asyncio
import aioodbc
import pandas as pd
 
class conn:
    async def connect_and_execute(self, dsn, text):
        conn = None  # Inicializa la variable conn como None
        try:
            async with aioodbc.connect(dsn=dsn) as conn:
                async with conn.cursor() as cur:
                    await cur.execute(text)
                    if text.strip().lower().startswith('select'):
                        rows = await cur.fetchall()
                        result = [dict(zip([column[0] for column in cur.description], row)) for row in rows]
                    else:
                        result = await cur.commit()
                    return result
        except Exception as ex:
            print(ex)
            return 0
        finally:
            if conn is not None:
                await conn.close()