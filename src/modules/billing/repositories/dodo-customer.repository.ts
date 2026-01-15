import { Inject, Injectable } from '@nestjs/common';
import { eq } from 'drizzle-orm';
import type { DrizzleDatabase } from '../../../database/drizzle/database';
import { dodoCustomers } from '../../../database/schemas/billing/dodo-customers.schema';
import type {
  DodoCustomer,
  NewDodoCustomer,
} from '../../../database/schemas/billing/dodo-customers.schema';

@Injectable()
export class DodoCustomerRepository {
  constructor(@Inject('DRIZZLE_DB') private readonly db: DrizzleDatabase) {}

  async create(data: NewDodoCustomer): Promise<DodoCustomer> {
    const [customer] = await this.db.insert(dodoCustomers).values(data).returning();
    return customer;
  }

  async findByUserId(userId: string): Promise<DodoCustomer | null> {
    const [customer] = await this.db
      .select()
      .from(dodoCustomers)
      .where(eq(dodoCustomers.userId, userId))
      .limit(1);

    return customer || null;
  }

  async findByDodoCustomerId(dodoCustomerId: string): Promise<DodoCustomer | null> {
    const [customer] = await this.db
      .select()
      .from(dodoCustomers)
      .where(eq(dodoCustomers.dodoCustomerId, dodoCustomerId))
      .limit(1);

    return customer || null;
  }

  async update(id: string, data: Partial<NewDodoCustomer>): Promise<DodoCustomer> {
    const [customer] = await this.db
      .update(dodoCustomers)
      .set({ ...data, updatedAt: new Date() })
      .where(eq(dodoCustomers.id, id))
      .returning();
    return customer;
  }

  async updateByUserId(
    userId: string,
    data: Partial<NewDodoCustomer>,
  ): Promise<DodoCustomer | null> {
    const [customer] = await this.db
      .update(dodoCustomers)
      .set({ ...data, updatedAt: new Date() })
      .where(eq(dodoCustomers.userId, userId))
      .returning();
    return customer || null;
  }

  async updateByDodoCustomerId(
    dodoCustomerId: string,
    data: Partial<NewDodoCustomer>,
  ): Promise<DodoCustomer | null> {
    const [customer] = await this.db
      .update(dodoCustomers)
      .set({ ...data, updatedAt: new Date() })
      .where(eq(dodoCustomers.dodoCustomerId, dodoCustomerId))
      .returning();
    return customer || null;
  }
}
