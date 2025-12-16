/**
 * User Service
 * Business logic for user management
 */

import { Injectable, NotFoundException, ConflictException } from '@nestjs/common';
import { UserRepository } from './repositories/user.repository';
import { createClient } from '@supabase/supabase-js';

@Injectable()
export class UserService {
  private supabase: ReturnType<typeof createClient>;
  private supabaseAdmin: ReturnType<typeof createClient> | null = null;

  constructor(private readonly userRepository: UserRepository) {
    const supabaseUrl = process.env.SUPABASE_URL;
    const supabaseAnonKey = process.env.SUPABASE_ANON_KEY;
    const supabaseServiceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

    if (supabaseUrl && supabaseAnonKey) {
      this.supabase = createClient(supabaseUrl, supabaseAnonKey, {
        auth: {
          autoRefreshToken: false,
          persistSession: false,
        },
      });
    }

    // Create admin client for admin operations (requires service role key)
    if (supabaseUrl && supabaseServiceRoleKey) {
      this.supabaseAdmin = createClient(supabaseUrl, supabaseServiceRoleKey, {
        auth: {
          autoRefreshToken: false,
          persistSession: false,
        },
      });
    }
  }

  /**
   * Create or update user from Supabase auth event
   */
  async createOrUpdateFromSupabase(supabaseUser: {
    id: string;
    email: string;
    email_confirmed_at?: string | null;
    user_metadata?: Record<string, unknown>;
    app_metadata?: Record<string, unknown>;
  }): Promise<{ user: any; created: boolean }> {
    // Check if user already exists
    const existingUser = await this.userRepository.findBySupabaseUserId(
      supabaseUser.id,
    );

    const userData = {
      id: supabaseUser.id,
      supabaseUserId: supabaseUser.id,
      email: supabaseUser.email,
      firstName: (supabaseUser.user_metadata?.first_name as string) || 
                 (supabaseUser.user_metadata?.firstName as string),
      lastName: (supabaseUser.user_metadata?.last_name as string) || 
                (supabaseUser.user_metadata?.lastName as string),
      fullName: (supabaseUser.user_metadata?.full_name as string) ||
               `${supabaseUser.user_metadata?.first_name || ''} ${supabaseUser.user_metadata?.last_name || ''}`.trim(),
      avatarUrl: (supabaseUser.user_metadata?.avatar_url as string) || 
                 (supabaseUser.user_metadata?.avatarUrl as string),
      emailVerified: !!supabaseUser.email_confirmed_at,
      metadata: {
        ...supabaseUser.user_metadata,
        ...supabaseUser.app_metadata,
      },
    };

    if (existingUser) {
      // Update existing user
      const updated = await this.userRepository.update(existingUser.id, userData);
      return { user: updated, created: false };
    } else {
      // Create new user
      const created = await this.userRepository.create({
        ...userData,
        status: 'active',
        onboardingCompleted: false,
        onboardingStep: 'welcome',
      });
      return { user: created, created: true };
    }
  }

  /**
   * Get user by ID
   * If user doesn't exist in database, try to sync from Supabase
   */
  async getUserById(id: string) {
    let user = await this.userRepository.findById(id);
    
    if (!user) {
      // User doesn't exist in database, try to sync from Supabase
      // Use admin client if available, otherwise try with regular client
      const client = this.supabaseAdmin || this.supabase;
      if (client) {
        try {
          // Try to get user from Supabase
          // Note: admin.getUserById requires service role key
          // If not available, we'll need to handle this differently
          let supabaseUser;
          if (this.supabaseAdmin) {
            const { data, error } = await this.supabaseAdmin.auth.admin.getUserById(id);
            if (!error && data?.user) {
              supabaseUser = data.user;
            }
          } else {
            // Fallback: try to get user info from the token
            // This is a workaround if service role key is not available
            console.warn('Service role key not configured. Cannot auto-sync user from Supabase.');
          }
          
          if (supabaseUser) {
            // Sync user from Supabase
            const result = await this.createOrUpdateFromSupabase({
              id: supabaseUser.id,
              email: supabaseUser.email || '',
              email_confirmed_at: supabaseUser.email_confirmed_at,
              user_metadata: supabaseUser.user_metadata || {},
              app_metadata: supabaseUser.app_metadata || {},
            });
            user = result.user;
          }
        } catch (error) {
          // If Supabase sync fails, log and continue to throw error
          console.error('Failed to sync user from Supabase:', error);
        }
      }
      
      // If still no user, throw error
      if (!user) {
        throw new NotFoundException(
          `User with ID "${id}" not found. Please ensure the user is synced from Supabase.`,
        );
      }
    }
    
    return user;
  }

  /**
   * Get user by Supabase user ID
   */
  async getUserBySupabaseId(supabaseUserId: string) {
    const user = await this.userRepository.findBySupabaseUserId(supabaseUserId);
    if (!user) {
      throw new NotFoundException(`User with Supabase ID "${supabaseUserId}" not found`);
    }
    return user;
  }

  /**
   * Update user
   */
  async updateUser(id: string, data: {
    firstName?: string;
    lastName?: string;
    fullName?: string;
    avatarUrl?: string;
    metadata?: Record<string, unknown>;
  }) {
    return this.userRepository.update(id, data);
  }

  /**
   * Update last login
   */
  async updateLastLogin(id: string) {
    await this.userRepository.updateLastLogin(id);
  }

  /**
   * Update onboarding status
   */
  async updateOnboarding(id: string, completed: boolean, step?: string) {
    return this.userRepository.updateOnboarding(id, completed, step);
  }

  /**
   * Set current organization
   */
  async setCurrentOrganization(userId: string, orgId: string | null) {
    return this.userRepository.setCurrentOrganization(userId, orgId);
  }
}
