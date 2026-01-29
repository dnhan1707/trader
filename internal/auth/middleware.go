package auth

import (
	"net/http"
	"strings"

	"github.com/gofiber/fiber/v2"
)

func Middleware(jwtSecret string) fiber.Handler {
	return func(c *fiber.Ctx) error {
		authHeader := c.Get("Authorization")
		if authHeader == "" {
			return c.Status(http.StatusUnauthorized).JSON(fiber.Map{
				"error": "missing token",
			})
		}

		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
			return c.Status(http.StatusUnauthorized).JSON(fiber.Map{"error": "invalid auth header"})
		}

		claims, err := ParseToken(parts[1], jwtSecret)
		if err != nil {
			return c.Status(http.StatusUnauthorized).JSON(fiber.Map{"error": "invalid token"})
		}

		c.Locals("userID", claims.UserId)
		return c.Next()
	}
}
