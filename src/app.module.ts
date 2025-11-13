import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { PostgresModule } from './modules/connectors/postgres/postgres.module';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      envFilePath: '.env',
      cache: true,
    }),
    PostgresModule,
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
