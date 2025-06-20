import { Handler, model } from 'dblink-core';
import mssql from 'mssql';
export default class MsSqlServer extends Handler {
  connectionPool;
  constructor(config) {
    super(config);
    this.connectionPool = new mssql.ConnectionPool(config);
  }
  async init() {
    await this.connectionPool.connect();
  }
  async getConnection() {
    await mssql.connect(this.config);
    return new mssql.Request();
  }
  async initTransaction() {}
  async commit() {}
  async rollback() {}
  async close() {}
  async run(query, dataArgs, connection) {
    const conn = connection ?? this.connectionPool.request();
    const data = await conn.query(query);
    const result = new model.ResultSet();
    result.rows = data.recordset;
    return result;
  }
  runStatement(queryStmt, connection) {
    const { query, dataArgs } = this.prepareQuery(queryStmt);
    return this.run(query, dataArgs, connection);
  }
  async stream(query, dataArgs, connection) {
    const conn = connection ?? this.connectionPool.request();
    conn.stream = true;
    conn.query(query);
    return conn.toReadableStream();
  }
  streamStatement(queryStmt, connection) {
    const { query, dataArgs } = this.prepareQuery(queryStmt);
    return this.stream(query, dataArgs, connection);
  }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaW5kZXguanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyJpbmRleC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQSxPQUFPLEVBQUUsT0FBTyxFQUFFLEtBQUssRUFBTyxNQUFNLGFBQWEsQ0FBQztBQUNsRCxPQUFPLEtBQUssTUFBTSxPQUFPLENBQUM7QUFXMUIsTUFBTSxDQUFDLE9BQU8sT0FBTyxXQUFZLFNBQVEsT0FBTztJQU05QyxjQUFjLENBQXVCO0lBUXJDLFlBQVksTUFBb0I7UUFDOUIsS0FBSyxDQUFDLE1BQU0sQ0FBQyxDQUFDO1FBRWQsSUFBSSxDQUFDLGNBQWMsR0FBRyxJQUFJLEtBQUssQ0FBQyxjQUFjLENBQUMsTUFBTSxDQUFDLENBQUM7SUFDekQsQ0FBQztJQVFELEtBQUssQ0FBQyxJQUFJO1FBQ1IsTUFBTSxJQUFJLENBQUMsY0FBYyxDQUFDLE9BQU8sRUFBRSxDQUFDO0lBQ3RDLENBQUM7SUFRRCxLQUFLLENBQUMsYUFBYTtRQUNqQixNQUFNLEtBQUssQ0FBQyxPQUFPLENBQUMsSUFBSSxDQUFDLE1BQXNCLENBQUMsQ0FBQztRQUNqRCxPQUFPLElBQUksS0FBSyxDQUFDLE9BQU8sRUFBRSxDQUFDO0lBQzdCLENBQUM7SUFTRCxLQUFLLENBQUMsZUFBZTtJQUdyQixDQUFDO0lBU0QsS0FBSyxDQUFDLE1BQU07SUFHWixDQUFDO0lBU0QsS0FBSyxDQUFDLFFBQVE7SUFHZCxDQUFDO0lBU0QsS0FBSyxDQUFDLEtBQUs7SUFHWCxDQUFDO0lBV0QsS0FBSyxDQUFDLEdBQUcsQ0FBQyxLQUFhLEVBQUUsUUFBb0IsRUFBRSxVQUEwQjtRQUN2RSxNQUFNLElBQUksR0FBRyxVQUFVLElBQUksSUFBSSxDQUFDLGNBQWMsQ0FBQyxPQUFPLEVBQUUsQ0FBQztRQUN6RCxNQUFNLElBQUksR0FBRyxNQUFNLElBQUksQ0FBQyxLQUFLLENBQUMsS0FBSyxDQUFDLENBQUM7UUFFckMsTUFBTSxNQUFNLEdBQUcsSUFBSSxLQUFLLENBQUMsU0FBUyxFQUFFLENBQUM7UUFDckMsTUFBTSxDQUFDLElBQUksR0FBRyxJQUFJLENBQUMsU0FBUyxDQUFDO1FBQzdCLE9BQU8sTUFBTSxDQUFDO0lBQ2hCLENBQUM7SUFTRCxZQUFZLENBQUMsU0FBMEMsRUFBRSxVQUEwQjtRQUNqRixNQUFNLEVBQUUsS0FBSyxFQUFFLFFBQVEsRUFBRSxHQUFHLElBQUksQ0FBQyxZQUFZLENBQUMsU0FBUyxDQUFDLENBQUM7UUFDekQsT0FBTyxJQUFJLENBQUMsR0FBRyxDQUFDLEtBQUssRUFBRSxRQUFRLEVBQUUsVUFBVSxDQUFDLENBQUM7SUFDL0MsQ0FBQztJQVdELEtBQUssQ0FBQyxNQUFNLENBQUMsS0FBYSxFQUFFLFFBQW9CLEVBQUUsVUFBMEI7UUFDMUUsTUFBTSxJQUFJLEdBQUcsVUFBVSxJQUFJLElBQUksQ0FBQyxjQUFjLENBQUMsT0FBTyxFQUFFLENBQUM7UUFDekQsSUFBSSxDQUFDLE1BQU0sR0FBRyxJQUFJLENBQUM7UUFFbkIsSUFBSSxDQUFDLEtBQUssQ0FBQyxLQUFLLENBQUMsQ0FBQztRQUNsQixPQUFPLElBQUksQ0FBQyxnQkFBZ0IsRUFBRSxDQUFDO0lBQ2pDLENBQUM7SUFTRCxlQUFlLENBQUMsU0FBMEMsRUFBRSxVQUEwQjtRQUNwRixNQUFNLEVBQUUsS0FBSyxFQUFFLFFBQVEsRUFBRSxHQUFHLElBQUksQ0FBQyxZQUFZLENBQUMsU0FBUyxDQUFDLENBQUM7UUFDekQsT0FBTyxJQUFJLENBQUMsTUFBTSxDQUFDLEtBQUssRUFBRSxRQUFRLEVBQUUsVUFBVSxDQUFDLENBQUM7SUFDbEQsQ0FBQztDQUNGIn0=
