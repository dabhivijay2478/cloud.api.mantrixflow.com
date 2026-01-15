import { IsInt, IsNotEmpty, Min } from 'class-validator';

export class ManageSeatsDto {
  @IsInt()
  @IsNotEmpty()
  @Min(0)
  seatCount: number; // Total number of seats desired (includes base seats)
}