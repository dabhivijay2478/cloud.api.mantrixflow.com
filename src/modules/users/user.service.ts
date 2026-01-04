/**
 * User Service
 * Business logic for user management
 */

import { forwardRef, Inject, Injectable, NotFoundException } from '@nestjs/common';
import { createClient } from '@supabase/supabase-js';
import { OrganizationMemberService } from '../organizations/organization-member.service';
import { UserRepository } from './repositories/user.repository';

@Injectable()
export class UserService {
  private supabase: ReturnType<typeof createClient>;
  private supabaseAdmin: ReturnType<typeof createClient> | null = null;

  constructor(
    private readonly userRepository: UserRepository,
    @Inject(forwardRef(() => OrganizationMemberService))
    private readonly memberService: OrganizationMemberService,
  ) {
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
    // Check if user already exists by Supabase user ID
    let existingUser = await this.userRepository.findBySupabaseUserId(supabaseUser.id);

    // Also check by email in case user exists but with different supabaseUserId
    // This can happen if user was invited before they signed up
    if (!existingUser) {
      existingUser = await this.userRepository.findByEmail(supabaseUser.email.toLowerCase());
    }

    const userData = {
      id: supabaseUser.id,
      supabaseUserId: supabaseUser.id,
      email: supabaseUser.email.toLowerCase(),
      firstName:
        (supabaseUser.user_metadata?.first_name as string) ||
        (supabaseUser.user_metadata?.firstName as string),
      lastName:
        (supabaseUser.user_metadata?.last_name as string) ||
        (supabaseUser.user_metadata?.lastName as string),
      fullName:
        (supabaseUser.user_metadata?.full_name as string) ||
        `${supabaseUser.user_metadata?.first_name || ''} ${supabaseUser.user_metadata?.last_name || ''}`.trim(),
      avatarUrl:
        (supabaseUser.user_metadata?.avatar_url as string) ||
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

      // Link user to any pending invites (if user was invited but not yet linked)
      try {
        const linkedMembers = await this.memberService.linkUserToInvite(
          supabaseUser.email.toLowerCase(),
          updated.id,
        );

        // If user was invited and doesn't have a current org, set it to the first one they were invited to
        if (linkedMembers.length > 0) {
          if (!updated.currentOrgId) {
            await this.userRepository.setCurrentOrganization(
              updated.id,
              linkedMembers[0].organizationId,
            );
          }
          // If user was invited, mark onboarding as completed (skip welcome page)
          if (!updated.onboardingCompleted) {
            await this.userRepository.updateOnboarding(updated.id, true);
          }
          // Reload user to get updated fields
          const reloaded = await this.userRepository.findById(updated.id);
          return { user: reloaded || updated, created: false };
        }
      } catch (error) {
        // Log error but don't fail user update if invite linking fails
        console.error('Failed to link user to invites:', error);
      }

      return { user: updated, created: false };
    } else {
      // Check if user was invited (has pending organization_members record)
      let wasInvited = false;
      try {
        const pendingInvites = await this.memberService.findAllInvitesByEmail(
          supabaseUser.email.toLowerCase(),
        );
        wasInvited = pendingInvites && pendingInvites.length > 0;
      } catch (error) {
        console.error('Error checking for invites:', error);
      }

      // Create new user
      const created = await this.userRepository.create({
        ...userData,
        status: 'active',
        // If user was invited, skip onboarding (they're already part of an org)
        onboardingCompleted: wasInvited,
        onboardingStep: wasInvited ? undefined : 'welcome',
      });

      // Link user to any pending invites (if user was invited before signing up)
      // This links the Supabase user to organization member records
      try {
        const linkedMembers = await this.memberService.linkUserToInvite(
          supabaseUser.email.toLowerCase(),
          created.id,
        );

        // If user was invited, set their current organization to the first one they were invited to
        if (linkedMembers.length > 0) {
          await this.userRepository.setCurrentOrganization(
            created.id,
            linkedMembers[0].organizationId,
          );
          // Mark onboarding as completed for invited users
          await this.userRepository.updateOnboarding(created.id, true);
          // Reload user to get updated fields
          const reloaded = await this.userRepository.findById(created.id);
          return { user: reloaded || created, created: true };
        }
      } catch (error) {
        // Log error but don't fail user creation if invite linking fails
        console.error('Failed to link user to invites:', error);
      }

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
          let supabaseUser: any;
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
            try {
              const result = await this.createOrUpdateFromSupabase({
                id: supabaseUser.id,
                email: supabaseUser.email || '',
                email_confirmed_at: supabaseUser.email_confirmed_at,
                user_metadata: supabaseUser.user_metadata || {},
                app_metadata: supabaseUser.app_metadata || {},
              });
              user = result.user;
            } catch (syncError) {
              // If sync fails with duplicate email error, try to find by email
              if (syncError && typeof syncError === 'object' && 'cause' in syncError) {
                const cause = (syncError as any).cause;
                if (
                  cause &&
                  typeof cause === 'object' &&
                  'code' in cause &&
                  cause.code === '23505'
                ) {
                  // Duplicate email error - user exists but with different supabaseUserId
                  // Try to find by email
                  try {
                    if (supabaseUser?.email) {
                      const existingUser = await this.userRepository.findByEmail(
                        supabaseUser.email.toLowerCase(),
                      );
                      if (existingUser) {
                        // Update the existing user with the new supabaseUserId
                        await this.userRepository.update(existingUser.id, {
                          supabaseUserId: supabaseUser.id,
                        });
                        user = await this.userRepository.findById(existingUser.id);
                      }
                    }
                  } catch (findError) {
                    console.error('Failed to find user by email after duplicate error:', findError);
                  }
                }
              }

              // If still no user, re-throw the error
              if (!user) {
                throw syncError;
              }
            }
          }
        } catch (error) {
          // If Supabase sync fails, log and continue to throw error
          if (!user) {
            console.error('Failed to sync user from Supabase:', error);
          }
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
  async updateUser(
    id: string,
    data: {
      firstName?: string;
      lastName?: string;
      fullName?: string;
      avatarUrl?: string;
      metadata?: Record<string, unknown>;
    },
  ) {
    // Update user in database
    const updatedUser = await this.userRepository.update(id, data);

    // Also update Supabase user metadata
    if (this.supabaseAdmin && updatedUser.supabaseUserId) {
      try {
        const userMetadata: Record<string, unknown> = {};
        if (data.firstName !== undefined) userMetadata.first_name = data.firstName;
        if (data.lastName !== undefined) userMetadata.last_name = data.lastName;
        if (data.fullName !== undefined) userMetadata.full_name = data.fullName;
        if (data.avatarUrl !== undefined) userMetadata.avatar_url = data.avatarUrl;

        await this.supabaseAdmin.auth.admin.updateUserById(updatedUser.supabaseUserId, {
          user_metadata: userMetadata,
        });
      } catch (error) {
        console.error('Failed to update Supabase user metadata:', error);
        // Don't fail the update if Supabase sync fails - database update succeeded
      }
    }

    return updatedUser;
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
