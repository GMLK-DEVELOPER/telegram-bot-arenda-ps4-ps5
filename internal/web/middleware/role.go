package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// RequireRole checks if the logged-in admin has one of the allowed roles.
// Usage: router.POST("/api/consoles", middleware.RequireRole("admin", "operator"), handler)
func RequireRole(allowedRoles ...string) gin.HandlerFunc {
	return func(c *gin.Context) {
		role, exists := c.Get("admin_role")
		if !exists {
			c.JSON(http.StatusForbidden, gin.H{"error": "Роль не определена"})
			c.Abort()
			return
		}

		userRole := role.(string)

		// superadmin has full access to everything
		if userRole == "superadmin" {
			c.Next()
			return
		}

		for _, allowed := range allowedRoles {
			if userRole == allowed {
				c.Next()
				return
			}
		}

		c.JSON(http.StatusForbidden, gin.H{"error": "Недостаточно прав для этого действия"})
		c.Abort()
	}
}
