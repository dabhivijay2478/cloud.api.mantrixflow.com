import { Injectable } from '@nestjs/common';

@Injectable()
export class AppService {
  getHello(): { message: string; docs: string } {
    return {
      message: 'MantrixFlow API is running',
      docs: '/api/docs',
    };
  }
}
